#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>

static inline uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static uint64_t xoshiro_next(uint64_t s[4]) {
    uint64_t result = rotl64(s[0] + s[3], 23) + s[0];
    uint64_t t = s[1] << 17;
    s[2] ^= s[0]; s[3] ^= s[1]; s[1] ^= s[2]; s[0] ^= s[3];
    s[2] ^= t; s[3] = rotl64(s[3], 45);
    return result;
}

static void xoshiro_seed(uint64_t s[4], uint64_t seed) {
    for (int i = 0; i < 4; i++) {
        seed += 0x9E3779B97F4A7C15ULL;
        uint64_t z = seed;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        s[i] = z ^ (z >> 31);
    }
}

#define KLL_MIN_LEVEL_SIZE 2

typedef struct {
    double  *items;
    int     *levels;
    int      num_levels;
    int      items_cap;
    int      levels_cap;
    int      cached_total_cap;
    uint32_t k;
    uint64_t total_n;
    double   min_val;
    double   max_val;
    uint64_t rng[4];
} kll_sketch_t;

static int kll_level_capacity(uint32_t k, int num_levels, int h) {
    int depth = num_levels - 1 - h;
    double cap = (double)k;
    for (int i = 0; i < depth; i++) cap *= (2.0 / 3.0);
    int c = (int)(cap + 0.5);
    return c < KLL_MIN_LEVEL_SIZE ? KLL_MIN_LEVEL_SIZE : c;
}

static int kll_total_capacity(uint32_t k, int num_levels) {
    int total = 0;
    for (int h = 0; h < num_levels; h++)
        total += kll_level_capacity(k, num_levels, h);
    return total;
}

static void kll_update_cached_cap(kll_sketch_t *sk) {
    sk->cached_total_cap = kll_total_capacity(sk->k, sk->num_levels);
}

/* Insertion sort for small n, qsort for large */
static inline void kll_sort(double *arr, int n) {
    if (n <= 1) return;
    if (n <= 32) {
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
        /* Branchless comparator for qsort */
        int cmp(const void *a, const void *b) {
            double da = *(const double *)a, db = *(const double *)b;
            return (da > db) - (da < db);
        }
        qsort(arr, (size_t)n, sizeof(double), cmp);
    }
}

static void kll_grow_items(kll_sketch_t *sk) {
    int old_cap = sk->items_cap;
    int grow_by = old_cap > (int)sk->k ? old_cap : (int)sk->k;
    int new_cap = old_cap + grow_by;
    double *new_items = (double *)malloc(new_cap * sizeof(double));
    int lo = sk->levels[0];
    int end = sk->levels[sk->num_levels];
    int used = end - lo;
    memcpy(new_items + lo + grow_by, sk->items + lo, used * sizeof(double));
    free(sk->items);
    sk->items = new_items;
    sk->items_cap = new_cap;
    for (int i = 0; i <= sk->num_levels; i++)
        sk->levels[i] += grow_by;
}

static void kll_add_level(kll_sketch_t *sk) {
    int nl = sk->num_levels + 1;
    if (nl + 1 > sk->levels_cap) {
        int new_cap = sk->levels_cap * 2;
        sk->levels = (int *)realloc(sk->levels, new_cap * sizeof(int));
        sk->levels_cap = new_cap;
    }
    sk->levels[nl] = sk->levels[sk->num_levels];
    sk->num_levels = nl;
    kll_update_cached_cap(sk);
}

static void kll_compact_level(kll_sketch_t *sk, int h) {
    int lo = sk->levels[h];
    int hi = sk->levels[h + 1];
    int sz = hi - lo;

    kll_sort(sk->items + lo, sz);

    int coin = (int)(xoshiro_next(sk->rng) & 1);
    int start = coin ? 1 : 0;
    int num_promoted = (sz - start + 1) / 2;
    int num_discarded = sz - num_promoted;

    int dst = 0;
    for (int src = start; src < sz; src += 2)
        sk->items[lo + dst++] = sk->items[lo + src];

    int end_all = sk->levels[sk->num_levels];
    int move_len = end_all - hi;
    if (move_len > 0 && hi != lo + num_promoted)
        memmove(sk->items + lo + num_promoted, sk->items + hi, move_len * sizeof(double));

    sk->levels[h + 1] = lo;
    for (int i = h + 2; i <= sk->num_levels; i++)
        sk->levels[i] -= num_discarded;
}

static void kll_compress(kll_sketch_t *sk) {
    for (int h = 0; h < sk->num_levels; h++) {
        int sz = sk->levels[h + 1] - sk->levels[h];
        int cap = kll_level_capacity(sk->k, sk->num_levels, h);
        if (sz >= cap && sz >= 2) {
            if (h + 1 >= sk->num_levels) kll_add_level(sk);
            kll_compact_level(sk, h);
        }
    }
}

kll_sketch_t *kll_new(uint32_t k, uint64_t seed) {
    kll_sketch_t *sk = (kll_sketch_t *)calloc(1, sizeof(kll_sketch_t));
    sk->k = k;
    int init_cap = k * 4;
    sk->items = (double *)malloc(init_cap * sizeof(double));
    sk->items_cap = init_cap;
    sk->levels = (int *)malloc(8 * sizeof(int));
    sk->levels_cap = 8;
    sk->levels[0] = init_cap;
    sk->levels[1] = init_cap;
    sk->num_levels = 1;
    sk->min_val = NAN;
    sk->max_val = NAN;
    xoshiro_seed(sk->rng, seed);
    kll_update_cached_cap(sk);
    return sk;
}

void kll_free(kll_sketch_t *sk) {
    if (sk) { free(sk->items); free(sk->levels); free(sk); }
}

void kll_insert(kll_sketch_t *sk, double val) {
    if (__builtin_expect(val != val, 0)) return;

    if (__builtin_expect(sk->total_n == 0, 0)) {
        sk->min_val = val; sk->max_val = val;
    } else {
        if (val < sk->min_val) sk->min_val = val;
        if (val > sk->max_val) sk->max_val = val;
    }

    if (__builtin_expect(sk->levels[0] <= 0, 0)) kll_grow_items(sk);
    sk->levels[0]--;
    sk->items[sk->levels[0]] = val;
    sk->total_n++;

    int retained = sk->levels[sk->num_levels] - sk->levels[0];
    if (__builtin_expect(retained >= sk->cached_total_cap, 0))
        kll_compress(sk);
}

uint64_t kll_count(const kll_sketch_t *sk)    { return sk->total_n; }
double   kll_min(const kll_sketch_t *sk)      { return sk->min_val; }
double   kll_max(const kll_sketch_t *sk)      { return sk->max_val; }
int      kll_is_empty(const kll_sketch_t *sk) { return sk->total_n == 0; }

int kll_retained(const kll_sketch_t *sk) {
    return sk->levels[sk->num_levels] - sk->levels[0];
}

double kll_rank(const kll_sketch_t *sk, double value) {
    if (sk->total_n == 0) return NAN;
    uint64_t count_below = 0;
    for (int h = 0; h < sk->num_levels; h++) {
        uint64_t weight = 1ULL << h;
        int lo = sk->levels[h], hi = sk->levels[h + 1];
        for (int i = lo; i < hi; i++)
            count_below += weight * (sk->items[i] < value);
    }
    return (double)count_below / (double)sk->total_n;
}

double kll_quantile(const kll_sketch_t *sk, double norm_rank) {
    if (sk->total_n == 0) return NAN;
    int retained = kll_retained(sk);
    typedef struct { double val; uint64_t weight; } witem;
    witem *witems = (witem *)malloc(retained * sizeof(witem));
    int wi = 0;
    for (int h = 0; h < sk->num_levels; h++) {
        uint64_t weight = 1ULL << h;
        int lo = sk->levels[h], hi = sk->levels[h + 1];
        for (int i = lo; i < hi; i++) {
            witems[wi].val = sk->items[i];
            witems[wi].weight = weight;
            wi++;
        }
    }
    int cmp(const void *a, const void *b) {
        double da = ((const witem *)a)->val, db = ((const witem *)b)->val;
        return (da > db) - (da < db);
    }
    qsort(witems, wi, sizeof(witem), cmp);
    uint64_t target = (uint64_t)(norm_rank * (double)sk->total_n);
    uint64_t cum = 0;
    double result = witems[wi - 1].val;
    for (int i = 0; i < wi; i++) {
        cum += witems[i].weight;
        if (cum > target) { result = witems[i].val; break; }
    }
    free(witems);
    return result;
}

void kll_merge(kll_sketch_t *dst, const kll_sketch_t *src) {
    if (src->total_n == 0) return;
    if (dst->total_n == 0) {
        dst->min_val = src->min_val; dst->max_val = src->max_val;
    } else {
        if (src->min_val < dst->min_val) dst->min_val = src->min_val;
        if (src->max_val > dst->max_val) dst->max_val = src->max_val;
    }
    dst->total_n += src->total_n;

    for (int h = 0; h < src->num_levels; h++) {
        int lo = src->levels[h], hi = src->levels[h + 1];
        int sz = hi - lo;
        if (sz == 0) continue;

        if (h == 0) {
            /* Bulk prepend to level 0 */
            while (dst->levels[0] < sz) kll_grow_items(dst);
            dst->levels[0] -= sz;
            memcpy(dst->items + dst->levels[0], src->items + lo, sz * sizeof(double));
        } else {
            while (dst->num_levels <= h) kll_add_level(dst);
            /* Bulk insert: shift higher levels right by sz, then copy */
            int end_all = dst->levels[dst->num_levels];
            while (end_all + sz > dst->items_cap) {
                int new_cap = dst->items_cap * 2;
                dst->items = (double *)realloc(dst->items, new_cap * sizeof(double));
                dst->items_cap = new_cap;
            }
            int hi_h = dst->levels[h + 1];
            int move_len = end_all - hi_h;
            if (move_len > 0)
                memmove(dst->items + hi_h + sz, dst->items + hi_h, move_len * sizeof(double));
            memcpy(dst->items + hi_h, src->items + lo, sz * sizeof(double));
            for (int j = h + 1; j <= dst->num_levels; j++)
                dst->levels[j] += sz;
        }
    }

    int retained = kll_retained(dst);
    if (retained >= dst->cached_total_cap) kll_compress(dst);
}
