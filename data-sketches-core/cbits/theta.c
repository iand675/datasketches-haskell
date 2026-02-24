#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static inline uint64_t theta_murmur_mix64(uint64_t h) {
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

#define THETA_MAX UINT64_MAX

typedef struct {
    uint64_t *entries;   /* kept sorted for O(log k) lookup */
    int       count;
    int       capacity;
    int       k;
    uint64_t  theta;
    int       is_empty;
} theta_sketch_t;

/* Binary search for hash in sorted entries. Returns 1 if found. */
static int theta_find(const theta_sketch_t *sk, uint64_t hash, int *insert_pos) {
    int lo = 0, hi = sk->count - 1;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) >> 1);
        uint64_t v = sk->entries[mid];
        if (v == hash) { *insert_pos = mid; return 1; }
        if (v < hash) lo = mid + 1;
        else hi = mid - 1;
    }
    *insert_pos = lo;
    return 0;
}

static void theta_ensure_cap(theta_sketch_t *sk) {
    if (sk->count >= sk->capacity) {
        sk->capacity *= 2;
        sk->entries = (uint64_t *)realloc(sk->entries, sk->capacity * sizeof(uint64_t));
    }
}

/* Insert into sorted position */
static void theta_sorted_insert(theta_sketch_t *sk, uint64_t hash) {
    int pos;
    if (theta_find(sk, hash, &pos)) return; /* duplicate */
    theta_ensure_cap(sk);
    /* Shift right to make room */
    if (pos < sk->count)
        memmove(sk->entries + pos + 1, sk->entries + pos,
                (sk->count - pos) * sizeof(uint64_t));
    sk->entries[pos] = hash;
    sk->count++;
}

static void theta_rebuild(theta_sketch_t *sk) {
    if (sk->count <= sk->k) return;
    /* Entries are already sorted. The k-th smallest is at index k-1. */
    sk->theta = sk->entries[sk->k - 1];
    /* Binary search for how many are strictly < theta */
    int lo = 0, hi = sk->count;
    while (lo < hi) {
        int mid = lo + ((hi - lo) >> 1);
        if (sk->entries[mid] < sk->theta) lo = mid + 1;
        else hi = mid;
    }
    sk->count = lo;
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
    sk->is_empty = 0;
    theta_sorted_insert(sk, hash);
    theta_rebuild(sk);
}

double theta_c_estimate(const theta_sketch_t *sk) {
    if (sk->is_empty) return 0.0;
    if (sk->theta == THETA_MAX) return (double)sk->count;
    return (double)sk->count / ((double)sk->theta / (double)THETA_MAX);
}

int theta_c_is_empty(const theta_sketch_t *sk) { return sk->is_empty; }

/* Insert a raw hash maintaining sorted order, for set operations */
static void theta_insert_hash(theta_sketch_t *sk, uint64_t hash) {
    if (hash == 0 || hash >= sk->theta) return;
    theta_sorted_insert(sk, hash);
}

theta_sketch_t *theta_c_union(const theta_sketch_t *a, const theta_sketch_t *b) {
    int k = a->k > b->k ? a->k : b->k;
    theta_sketch_t *r = theta_new(k);
    uint64_t min_theta = a->theta < b->theta ? a->theta : b->theta;
    r->theta = min_theta;
    if (!a->is_empty) {
        r->is_empty = 0;
        for (int i = 0; i < a->count && a->entries[i] < min_theta; i++)
            theta_insert_hash(r, a->entries[i]);
    }
    if (!b->is_empty) {
        r->is_empty = 0;
        for (int i = 0; i < b->count && b->entries[i] < min_theta; i++)
            theta_insert_hash(r, b->entries[i]);
    }
    theta_rebuild(r);
    return r;
}

theta_sketch_t *theta_c_intersection(const theta_sketch_t *a, const theta_sketch_t *b) {
    int k = a->k > b->k ? a->k : b->k;
    theta_sketch_t *r = theta_new(k);
    if (a->is_empty || b->is_empty) return r;
    uint64_t min_theta = a->theta < b->theta ? a->theta : b->theta;
    r->theta = min_theta;
    r->is_empty = 0;
    for (int i = 0; i < a->count && a->entries[i] < min_theta; i++) {
        int pos;
        if (theta_find(b, a->entries[i], &pos))
            theta_insert_hash(r, a->entries[i]);
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
    for (int i = 0; i < a->count && a->entries[i] < min_theta; i++) {
        int pos;
        if (!theta_find(b, a->entries[i], &pos))
            theta_insert_hash(r, a->entries[i]);
    }
    theta_rebuild(r);
    return r;
}
