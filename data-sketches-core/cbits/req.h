#ifndef REQ_H
#define REQ_H

#include <stdint.h>

typedef struct req_sketch req_sketch_t;

req_sketch_t *req_new(uint32_t k, int rank_accuracy, uint64_t seed);
void          req_free(req_sketch_t *sk);

void     req_insert(req_sketch_t *sk, double val);
void     req_merge(req_sketch_t *dst, const req_sketch_t *src);

uint64_t req_count(const req_sketch_t *sk);
int      req_is_empty(const req_sketch_t *sk);
double   req_min(const req_sketch_t *sk);
double   req_max(const req_sketch_t *sk);
double   req_sum(const req_sketch_t *sk);
int      req_retained(const req_sketch_t *sk);
uint32_t req_k(const req_sketch_t *sk);
int      req_rank_accuracy(const req_sketch_t *sk);
int      req_num_levels(const req_sketch_t *sk);
int      req_criterion(const req_sketch_t *sk);
void     req_set_criterion(req_sketch_t *sk, int crit);

uint64_t req_count_with_criterion(req_sketch_t *sk, double value);
double   req_rank(req_sketch_t *sk, double value);
double   req_quantile(req_sketch_t *sk, double norm_rank);

#endif
