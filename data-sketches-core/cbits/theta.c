#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ========================================================================
 * Theta Sketch — entire implementation in C
 * ======================================================================== */

static inline uint64_t theta_murmur_mix64(uint64_t h) {
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

#define THETA_MAX UINT64_MAX

typedef struct {
    uint64_t *entries;
    int       count;
    int       capacity;
    int       k;
    uint64_t  theta;
    int       is_empty;
} theta_sketch_t;

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static int theta_check_dup(const theta_sketch_t *sk, uint64_t hash) {
    for (int i = 0; i < sk->count; i++)
        if (sk->entries[i] == hash) return 1;
    return 0;
}

static void theta_rebuild(theta_sketch_t *sk) {
    if (sk->count <= sk->k) return;
    qsort(sk->entries, sk->count, sizeof(uint64_t), cmp_u64);
    uint64_t new_theta = sk->entries[sk->k - 1];
    int below = 0;
    for (int i = 0; i < sk->count; i++) {
        if (sk->entries[i] < new_theta) below++;
        else break;
    }
    sk->theta = new_theta;
    sk->count = below;
}

static void theta_ensure_cap(theta_sketch_t *sk) {
    if (sk->count >= sk->capacity) {
        int new_cap = sk->capacity * 2;
        sk->entries = (uint64_t *)realloc(sk->entries, new_cap * sizeof(uint64_t));
        sk->capacity = new_cap;
    }
}

theta_sketch_t *theta_new(int k) {
    theta_sketch_t *sk = (theta_sketch_t *)calloc(1, sizeof(theta_sketch_t));
    sk->k = k;
    sk->capacity = k * 2;
    sk->entries = (uint64_t *)malloc(sk->capacity * sizeof(uint64_t));
    sk->theta = THETA_MAX;
    sk->is_empty = 1;
    return sk;
}

void theta_free(theta_sketch_t *sk) {
    if (sk) { free(sk->entries); free(sk); }
}

void theta_c_insert(theta_sketch_t *sk, uint64_t item) {
    uint64_t hash = theta_murmur_mix64(item);
    if (hash == 0 || hash >= sk->theta) return;
    if (theta_check_dup(sk, hash)) return;
    sk->is_empty = 0;
    theta_ensure_cap(sk);
    sk->entries[sk->count++] = hash;
    theta_rebuild(sk);
}

double theta_c_estimate(const theta_sketch_t *sk) {
    if (sk->is_empty) return 0.0;
    if (sk->theta == THETA_MAX) return (double)sk->count;
    return (double)sk->count / ((double)sk->theta / (double)THETA_MAX);
}

int theta_c_is_empty(const theta_sketch_t *sk) { return sk->is_empty; }

/* Insert a raw hash (no re-hashing), for set operations */
static void theta_insert_hash(theta_sketch_t *sk, uint64_t hash) {
    if (hash == 0 || hash >= sk->theta) return;
    if (theta_check_dup(sk, hash)) return;
    theta_ensure_cap(sk);
    sk->entries[sk->count++] = hash;
}

theta_sketch_t *theta_c_union(const theta_sketch_t *a, const theta_sketch_t *b) {
    int k = a->k > b->k ? a->k : b->k;
    theta_sketch_t *r = theta_new(k);
    uint64_t min_theta = a->theta < b->theta ? a->theta : b->theta;
    r->theta = min_theta;

    if (!a->is_empty) {
        r->is_empty = 0;
        for (int i = 0; i < a->count; i++)
            if (a->entries[i] < min_theta) theta_insert_hash(r, a->entries[i]);
    }
    if (!b->is_empty) {
        r->is_empty = 0;
        for (int i = 0; i < b->count; i++)
            if (b->entries[i] < min_theta) theta_insert_hash(r, b->entries[i]);
    }
    theta_rebuild(r);
    return r;
}

static int theta_contains(const theta_sketch_t *sk, uint64_t hash) {
    for (int i = 0; i < sk->count; i++)
        if (sk->entries[i] == hash) return 1;
    return 0;
}

theta_sketch_t *theta_c_intersection(const theta_sketch_t *a, const theta_sketch_t *b) {
    int k = a->k > b->k ? a->k : b->k;
    theta_sketch_t *r = theta_new(k);
    if (a->is_empty || b->is_empty) return r;

    uint64_t min_theta = a->theta < b->theta ? a->theta : b->theta;
    r->theta = min_theta;
    r->is_empty = 0;

    for (int i = 0; i < a->count; i++) {
        uint64_t h = a->entries[i];
        if (h < min_theta && theta_contains(b, h))
            theta_insert_hash(r, h);
    }
    theta_rebuild(r);
    return r;
}

theta_sketch_t *theta_c_difference(const theta_sketch_t *a, const theta_sketch_t *b) {
    theta_sketch_t *r = theta_new(a->k);
    if (a->is_empty) return r;

    uint64_t min_theta = a->theta < b->theta ? a->theta : b->theta;
    r->theta = min_theta;
    r->is_empty = 0;

    for (int i = 0; i < a->count; i++) {
        uint64_t h = a->entries[i];
        if (h < min_theta && !theta_contains(b, h))
            theta_insert_hash(r, h);
    }
    theta_rebuild(r);
    return r;
}
