#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/*
 * Portable SIMD-friendly C implementations of sketch hot paths.
 *
 * All loops are written as straight-line iterations over contiguous arrays
 * with no data-dependent branches in the inner loop body. This lets
 * GCC/Clang auto-vectorize with -O2:
 *   - x86-64: SSE2/AVX2 (128/256-bit)
 *   - ARM:    NEON (128-bit)
 *
 * Compile with: -O2 -march=native (or -march=armv8-a+simd on ARM)
 */

/* ========================================================================
 * Murmur3 64-bit finalizer (same hash as the Haskell version)
 * ======================================================================== */

static inline uint64_t murmur_mix64(uint64_t h) {
    h ^= h >> 33;
    h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33;
    h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

/* Count trailing zeros, portable */
static inline int ctz64(uint64_t x) {
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll(x);
#else
    int n = 0;
    while ((x & 1) == 0) { x >>= 1; n++; }
    return n;
#endif
}

/* ========================================================================
 * HyperLogLog
 * ======================================================================== */

/*
 * Insert a single item into an HLL sketch.
 * registers: pointer to register array (uint8_t[m])
 * p:         precision (log2 of register count)
 * item:      the value to insert (pre-hashed or raw)
 */
void hll_insert(uint8_t *registers, int p, uint64_t item) {
    uint64_t hash = murmur_mix64(item);
    int m = 1 << p;
    int reg_idx = (int)(hash & (uint64_t)(m - 1));
    uint64_t w = hash >> p;
    int bits = 64 - p;
    int rho;
    if (w == 0) {
        rho = bits + 1;
    } else {
        int clz = ctz64(w);
        rho = (clz < bits ? clz : bits) + 1;
    }
    uint8_t rho8 = (uint8_t)rho;
    if (rho8 > registers[reg_idx]) {
        registers[reg_idx] = rho8;
    }
}

/*
 * Bulk insert N items into an HLL sketch.
 * Avoids Haskell→C FFI overhead per item.
 */
void hll_insert_bulk(uint8_t *registers, int p, const uint64_t *items, int n) {
    for (int i = 0; i < n; i++) {
        hll_insert(registers, p, items[i]);
    }
}

/*
 * Compute the HLL estimate from a register array.
 *
 * Returns the raw estimate, linear counting estimate, and zero count
 * so the caller can apply the appropriate correction.
 *
 * The inner loop computes sum(2^(-register[i])) using ldexp, which
 * compiles to a single x87/SSE instruction. The Haskell version was
 * using Integer-based (^^) which is 100x slower.
 *
 * The loop is auto-vectorizable: no branches, pure accumulation.
 */
void hll_estimate(const uint8_t *registers, int m, double *out_raw_estimate,
                  double *out_harmonic_sum, int *out_zero_count) {
    double harmonic_sum = 0.0;
    int zero_count = 0;
    double mf = (double)m;

    /* SIMD-friendly: straight-line accumulation, no branches */
    for (int i = 0; i < m; i++) {
        int val = (int)registers[i];
        harmonic_sum += ldexp(1.0, -val); /* 1.0 / 2^val */
        zero_count += (val == 0);
    }

    /* alpha_m bias correction */
    double alpha;
    if (m == 16)      alpha = 0.673;
    else if (m == 32) alpha = 0.697;
    else if (m == 64) alpha = 0.709;
    else              alpha = 0.7213 / (1.0 + 1.079 / mf);

    double raw = alpha * mf * mf / harmonic_sum;

    *out_raw_estimate = raw;
    *out_harmonic_sum = harmonic_sum;
    *out_zero_count = zero_count;
}

/* ========================================================================
 * Count-Min Sketch
 * ======================================================================== */

/*
 * Insert an item into a Count-Min sketch.
 * table:  pointer to the table (uint64_t[rows * cols])
 * seeds:  pointer to per-row seeds (uint64_t[rows])
 * rows:   number of hash functions (depth)
 * cols:   width of each row
 * item:   the item hash
 * count:  the count to add
 *
 * Inner loop: rows is typically 5-7, fully unrolled by the compiler.
 */
void cms_insert(uint64_t *table, const uint64_t *seeds,
                int rows, int cols, uint64_t item, uint64_t count) {
    for (int r = 0; r < rows; r++) {
        uint64_t h = murmur_mix64(seeds[r] ^ item);
        int col = (int)(h % (uint64_t)cols);
        table[r * cols + col] += count;
    }
}

/*
 * Estimate the count of an item in a Count-Min sketch.
 * Returns the minimum across all rows.
 */
uint64_t cms_estimate(const uint64_t *table, const uint64_t *seeds,
                      int rows, int cols, uint64_t item) {
    uint64_t min_val = UINT64_MAX;
    for (int r = 0; r < rows; r++) {
        uint64_t h = murmur_mix64(seeds[r] ^ item);
        int col = (int)(h % (uint64_t)cols);
        uint64_t val = table[r * cols + col];
        if (val < min_val) min_val = val;
    }
    return min_val;
}

/* ========================================================================
 * xoshiro256++ fast PRNG (Blackman & Vigna, public domain)
 *
 * 4 x uint64_t state = 32 bytes.
 * Period: 2^256 - 1.
 * Passes BigCrush. Much faster than Mersenne Twister.
 * Supports both x86-64 and ARM (pure C, no platform-specific code).
 * ======================================================================== */

static inline uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

uint64_t xoshiro256pp_next(uint64_t state[4]) {
    uint64_t result = rotl64(state[0] + state[3], 23) + state[0];
    uint64_t t = state[1] << 17;
    state[2] ^= state[0];
    state[3] ^= state[1];
    state[1] ^= state[2];
    state[0] ^= state[3];
    state[2] ^= t;
    state[3] = rotl64(state[3], 45);
    return result;
}

/* Generate a random boolean (coin flip) from xoshiro state */
int xoshiro256pp_coin(uint64_t state[4]) {
    return (int)(xoshiro256pp_next(state) & 1);
}

/* Seed xoshiro256++ from a single uint64 using splitmix64 */
void xoshiro256pp_seed(uint64_t state[4], uint64_t seed) {
    for (int i = 0; i < 4; i++) {
        seed += 0x9E3779B97F4A7C15ULL;
        uint64_t z = seed;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z = z ^ (z >> 31);
        state[i] = z;
    }
}

/* ========================================================================
 * KLL / REQ sort helper
 *
 * Insertion sort for small arrays (< 32 elements). Falls back to
 * the system qsort for larger arrays. KLL compaction sorts level
 * buffers which are often small enough for insertion sort to win.
 * ======================================================================== */

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

void sort_doubles(double *arr, int n) {
    if (n <= 1) return;
    if (n <= 32) {
        /* Insertion sort: branch-free inner loop, good for small n */
        for (int i = 1; i < n; i++) {
            double key = arr[i];
            int j = i - 1;
            while (j >= 0 && arr[j] > key) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = key;
        }
    } else {
        qsort(arr, (size_t)n, sizeof(double), cmp_double);
    }
}
