#ifndef KLL_H
#define KLL_H

#include <stdint.h>

typedef struct kll_sketch kll_sketch_t;

kll_sketch_t *kll_new(uint32_t k, uint64_t seed);
void kll_free(kll_sketch_t *sk);
void kll_insert(kll_sketch_t *sk, double val);
uint64_t kll_count(const kll_sketch_t *sk);
double kll_min(const kll_sketch_t *sk);
double kll_max(const kll_sketch_t *sk);
int kll_is_empty(const kll_sketch_t *sk);
int kll_retained(const kll_sketch_t *sk);
double kll_rank(const kll_sketch_t *sk, double value);
double kll_quantile(const kll_sketch_t *sk, double norm_rank);
void kll_merge(kll_sketch_t *dst, const kll_sketch_t *src);

#endif
