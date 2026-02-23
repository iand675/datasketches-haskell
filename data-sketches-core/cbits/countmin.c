#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ========================================================================
 * Count-Min Sketch — entire implementation in C
 * ======================================================================== */

static inline uint64_t cms_murmur_mix(uint64_t h) {
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

typedef struct {
    uint64_t *table;
    uint64_t *seeds;
    int       cols;
    int       rows;
    uint64_t  total_n;
} cms_sketch_t;

static void cms_generate_seeds(uint64_t *seeds, int d) {
    for (int i = 0; i < d; i++)
        seeds[i] = cms_murmur_mix((uint64_t)i * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL);
}

cms_sketch_t *cms_new(double epsilon, double delta) {
    int w = (int)ceil(exp(1.0) / epsilon);
    int d = (int)ceil(log(1.0 / delta));
    cms_sketch_t *sk = (cms_sketch_t *)calloc(1, sizeof(cms_sketch_t));
    sk->cols = w;
    sk->rows = d;
    sk->table = (uint64_t *)calloc(w * d, sizeof(uint64_t));
    sk->seeds = (uint64_t *)malloc(d * sizeof(uint64_t));
    cms_generate_seeds(sk->seeds, d);
    sk->total_n = 0;
    return sk;
}

void cms_free(cms_sketch_t *sk) {
    if (sk) { free(sk->table); free(sk->seeds); free(sk); }
}

void cms_c_insert(cms_sketch_t *sk, uint64_t item, uint64_t count) {
    for (int r = 0; r < sk->rows; r++) {
        uint64_t h = cms_murmur_mix(sk->seeds[r] ^ item);
        int col = (int)(h % (uint64_t)sk->cols);
        sk->table[r * sk->cols + col] += count;
    }
    sk->total_n += count;
}

uint64_t cms_c_estimate(const cms_sketch_t *sk, uint64_t item) {
    uint64_t min_val = UINT64_MAX;
    for (int r = 0; r < sk->rows; r++) {
        uint64_t h = cms_murmur_mix(sk->seeds[r] ^ item);
        int col = (int)(h % (uint64_t)sk->cols);
        uint64_t val = sk->table[r * sk->cols + col];
        if (val < min_val) min_val = val;
    }
    return min_val;
}

void cms_c_merge(cms_sketch_t *dst, const cms_sketch_t *src) {
    int n = dst->rows * dst->cols;
    for (int i = 0; i < n; i++)
        dst->table[i] += src->table[i];
    dst->total_n += src->total_n;
}

int cms_c_width(const cms_sketch_t *sk)  { return sk->cols; }
int cms_c_depth(const cms_sketch_t *sk)  { return sk->rows; }
