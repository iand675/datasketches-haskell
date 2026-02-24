#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static inline uint64_t hll_murmur_mix64(uint64_t h) {
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

typedef struct {
    uint8_t *registers;
    int      p;
    int      m;
} hll_sketch_t;

hll_sketch_t *hll_new(int p) {
    hll_sketch_t *sk = (hll_sketch_t *)calloc(1, sizeof(hll_sketch_t));
    sk->p = p;
    sk->m = 1 << p;
    sk->registers = (uint8_t *)calloc(sk->m, 1);
    return sk;
}

void hll_free(hll_sketch_t *sk) {
    if (sk) { free(sk->registers); free(sk); }
}

void hll_c_insert(hll_sketch_t *sk, uint64_t item) {
    uint64_t hash = hll_murmur_mix64(item);
    int reg_idx = (int)(hash & (uint64_t)(sk->m - 1));
    /* Branchless rho: place a sentinel bit at position (64-p) so
       __builtin_ctzll always terminates within the valid range.
       Eliminates two branches from the old w==0 / clz<bits checks. */
    int bits = 64 - sk->p;
    uint64_t w = (hash >> sk->p) | (1ULL << bits);
    uint8_t rho = (uint8_t)(__builtin_ctzll(w) + 1);
    uint8_t cur = sk->registers[reg_idx];
    sk->registers[reg_idx] = rho > cur ? rho : cur;
}

void hll_c_insert_batch(hll_sketch_t *sk, const uint64_t *items, int n) {
    for (int i = 0; i < n; i++)
        hll_c_insert(sk, items[i]);
}

double hll_c_estimate(const hll_sketch_t *sk) {
    int m = sk->m;
    double mf = (double)m;
    double harmonic_sum = 0.0;
    int zero_count = 0;

    /* Auto-vectorizable: straight-line accumulation, no data-dependent branches */
    for (int i = 0; i < m; i++) {
        int val = (int)sk->registers[i];
        harmonic_sum += ldexp(1.0, -val);
        zero_count += (val == 0);
    }

    double alpha;
    if      (m == 16) alpha = 0.673;
    else if (m == 32) alpha = 0.697;
    else if (m == 64) alpha = 0.709;
    else              alpha = 0.7213 / (1.0 + 1.079 / mf);

    double raw = alpha * mf * mf / harmonic_sum;
    double twoTo32 = 4294967296.0;

    if (raw <= 2.5 * mf && zero_count > 0)
        return mf * log(mf / (double)zero_count);
    else if (raw > twoTo32 / 30.0)
        return -twoTo32 * log(1.0 - raw / twoTo32);
    else
        return raw;
}

/* Branchless register-wise max for auto-vectorization */
void hll_c_merge(hll_sketch_t *dst, const hll_sketch_t *src) {
    int m = dst->m;
    uint8_t *__restrict__ d = dst->registers;
    const uint8_t *__restrict__ s = src->registers;
    for (int i = 0; i < m; i++) {
        uint8_t sv = s[i], dv = d[i];
        d[i] = sv > dv ? sv : dv;
    }
}

int hll_c_precision(const hll_sketch_t *sk) { return sk->p; }
