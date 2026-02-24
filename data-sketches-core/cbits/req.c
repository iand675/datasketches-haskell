#include "req.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <assert.h>

/* ── Constants ─────────────────────────────────────────────────────── */
#define INIT_NUM_SECTIONS 3
#define MIN_K             4
#define NOM_CAP_MULT      2
#define SQRT2             1.4142135623730951

/* ── RNG (xoshiro256++) ────────────────────────────────────────────── */
static inline uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

static uint64_t xo_next(uint64_t s[4]) {
    uint64_t r = rotl64(s[0] + s[3], 23) + s[0];
    uint64_t t = s[1] << 17;
    s[2] ^= s[0]; s[3] ^= s[1]; s[1] ^= s[2]; s[0] ^= s[3];
    s[2] ^= t;    s[3] = rotl64(s[3], 45);
    return r;
}

static void xo_seed(uint64_t s[4], uint64_t seed) {
    for (int i = 0; i < 4; i++) {
        seed += 0x9E3779B97F4A7C15ULL;
        uint64_t z = seed;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        s[i] = z ^ (z >> 31);
    }
}

/* ── Sort ─────────────────────────────────────────────────────────── */
static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static inline void dswap(double *a, double *b) {
    double t = *a; *a = *b; *b = t;
}

static void isort(double *arr, int n) {
    for (int i = 1; i < n; i++) {
        double key = arr[i];
        int j = i - 1;
        while (j >= 0 && arr[j] > key) { arr[j + 1] = arr[j]; j--; }
        arr[j + 1] = key;
    }
}

static void dqsort(double *arr, int n) {
    while (n > 16) {
        int mid = n >> 1;
        if (arr[0] > arr[mid]) dswap(&arr[0], &arr[mid]);
        if (arr[0] > arr[n-1]) dswap(&arr[0], &arr[n-1]);
        if (arr[mid] > arr[n-1]) dswap(&arr[mid], &arr[n-1]);
        double pivot = arr[mid];
        dswap(&arr[mid], &arr[n-2]);
        int i = 0, j = n - 2;
        for (;;) {
            while (arr[++i] < pivot) {}
            while (arr[--j] > pivot) {}
            if (i >= j) break;
            dswap(&arr[i], &arr[j]);
        }
        dswap(&arr[i], &arr[n-2]);
        if (i < n - i) { dqsort(arr, i); arr += i + 1; n -= i + 1; }
        else { dqsort(arr + i + 1, n - i - 1); n = i; }
    }
    isort(arr, n);
}

static void sort_range(double *arr, int lo, int hi) {
    int n = hi - lo;
    if (n <= 1) return;
    if (n <= 16) { isort(arr + lo, n); return; }
    dqsort(arr + lo, n);
}

/* ── DoubleBuffer ──────────────────────────────────────────────────── */
typedef struct {
    double *data;
    int     count;
    int     capacity;
    int     growth;
    int     sorted;
    int     sab;          /* space_at_bottom: 1 for HRA, 0 for LRA */
} buf_t;

static void buf_init(buf_t *b, int cap, int growth, int sab) {
    b->data     = (double *)malloc(cap * sizeof(double));
    b->count    = 0;
    b->capacity = cap;
    b->growth   = growth;
    b->sorted   = 1;
    b->sab      = sab;
}

static void buf_free(buf_t *b) { free(b->data); b->data = NULL; }

static void buf_ensure_cap(buf_t *b, int need) {
    if (need <= b->capacity) return;
    double *nd = (double *)malloc(need * sizeof(double));
    if (b->sab) {
        int sp = b->capacity - b->count;
        int dp = need - b->count;
        memcpy(nd + dp, b->data + sp, b->count * sizeof(double));
    } else {
        memcpy(nd, b->data, b->count * sizeof(double));
    }
    free(b->data);
    b->data = nd;
    b->capacity = need;
}

static inline void buf_ensure_space(buf_t *b, int extra) {
    if (b->count + extra > b->capacity)
        buf_ensure_cap(b, b->count + extra + b->growth);
}

static inline void buf_append(buf_t *b, double val) {
    buf_ensure_space(b, 1);
    int ix = b->sab ? b->capacity - b->count - 1 : b->count;
    b->data[ix] = val;
    b->count++;
    b->sorted = 0;
}

static void buf_sort(buf_t *b) {
    if (b->sorted) return;
    if (b->sab) sort_range(b->data, b->capacity - b->count, b->capacity);
    else         sort_range(b->data, 0, b->count);
    b->sorted = 1;
}

static inline void buf_trim(buf_t *b, int nc) {
    if (nc < b->count) b->count = nc;
}

static buf_t buf_copy(const buf_t *src) {
    buf_t dst;
    dst.capacity = src->capacity;
    dst.count    = src->count;
    dst.growth   = src->growth;
    dst.sorted   = src->sorted;
    dst.sab      = src->sab;
    dst.data     = (double *)malloc(dst.capacity * sizeof(double));
    memcpy(dst.data, src->data, dst.capacity * sizeof(double));
    return dst;
}

/* Extract every-other element from [start_off, end_off) in logical coords. */
static buf_t buf_evens_or_odds(buf_t *b, int so, int eo, int odds) {
    buf_sort(b);
    int start, end;
    if (b->sab) {
        int base = b->capacity - b->count;
        start = base + so; end = base + eo;
    } else {
        start = so; end = eo;
    }
    int range = eo - so;
    int out_n = range / 2;
    buf_t r;
    r.data     = (double *)malloc(out_n * sizeof(double));
    r.count    = out_n;
    r.capacity = out_n;
    r.growth   = 0;
    r.sorted   = 1;
    r.sab      = b->sab;

    int off = odds ? 1 : 0;
    for (int i = start, j = 0; j < out_n; i += 2, j++)
        r.data[j] = b->data[i + off];
    return r;
}

/* Merge sorted src into sorted dst (in-place in dst). */
static void buf_merge_in(buf_t *dst, buf_t *src) {
    buf_sort(dst);
    buf_sort(src);
    int sl = src->count;
    buf_ensure_space(dst, sl);
    int tl = dst->count + sl;

    if (dst->sab) {
        int dc = dst->capacity, sc = src->capacity;
        int i = dc - dst->count, j = sc - sl;
        int k = dc - tl;
        while (k < dc) {
            if      (i < dc && j < sc) { if (dst->data[i] <= src->data[j]) dst->data[k++] = dst->data[i++]; else dst->data[k++] = src->data[j++]; }
            else if (i < dc)           dst->data[k++] = dst->data[i++];
            else if (j < sc)           dst->data[k++] = src->data[j++];
            else break;
        }
    } else {
        int i = dst->count - 1, j = sl - 1, k = tl - 1;
        while (k >= 0) {
            if      (i >= 0 && j >= 0) { if (dst->data[i] >= src->data[j]) dst->data[k--] = dst->data[i--]; else dst->data[k--] = src->data[j--]; }
            else if (i >= 0)           dst->data[k--] = dst->data[i--];
            else if (j >= 0)           dst->data[k--] = src->data[j--];
            else break;
        }
    }
    dst->count = tl;
    dst->sorted = 1;
}

/* Count items in buffer matching criterion (0=LT, 1=LE) via binary search. */
static int buf_count_crit(buf_t *b, double val, int crit) {
    buf_sort(b);
    int lo, hi;
    if (b->sab) { lo = b->capacity - b->count; hi = b->capacity; }
    else         { lo = 0; hi = b->count; }

    int left = lo, right = hi;
    if (crit == 0) {
        while (left < right) { int m = left + (right - left) / 2; if (b->data[m] < val) left = m + 1; else right = m; }
    } else {
        while (left < right) { int m = left + (right - left) / 2; if (b->data[m] <= val) left = m + 1; else right = m; }
    }
    return left - lo;
}

/* ── Compactor ─────────────────────────────────────────────────────── */
typedef struct {
    buf_t    buf;
    uint64_t state;
    double   sec_flt;
    int      sec_sz;
    int      num_sec;
    int      last_flip;
    int      hra;           /* 1=HRA, 0=LRA */
    uint8_t  lg_wt;
} compactor_t;

static inline int comp_nom_cap(const compactor_t *c) {
    return NOM_CAP_MULT * c->num_sec * c->sec_sz;
}

static void comp_init(compactor_t *c, uint8_t lgw, int hra, uint32_t ssz) {
    int nc = NOM_CAP_MULT * INIT_NUM_SECTIONS * (int)ssz;
    buf_init(&c->buf, nc * 2, nc, hra);
    c->state     = 0;
    c->sec_flt   = (double)ssz;
    c->sec_sz    = (int)ssz;
    c->num_sec   = INIT_NUM_SECTIONS;
    c->last_flip = 0;
    c->hra       = hra;
    c->lg_wt     = lgw;
}

static void comp_free(compactor_t *c) { buf_free(&c->buf); }

static inline int nearest_even(double x) {
    return (int)(round(x / 2.0)) << 1;
}

static int comp_ensure_sections(compactor_t *c) {
    double szf = c->sec_flt / SQRT2;
    int ne = nearest_even(szf);
    if (c->state >= (1ULL << (c->num_sec - 1)) && c->sec_sz > MIN_K && ne >= MIN_K) {
        c->sec_flt = szf;
        c->sec_sz  = ne;
        c->num_sec <<= 1;
        buf_ensure_cap(&c->buf, 2 * comp_nom_cap(c));
        return 1;
    }
    return 0;
}

typedef struct { int dri; int dns; buf_t promoted; } compact_ret_t;

static compact_ret_t comp_compact(compactor_t *c, uint64_t rng[4]) {
    int s0 = c->buf.count;
    int nc0 = comp_nom_cap(c);

    uint64_t cs = ~c->state;
    int to = cs == 0 ? 65 : __builtin_ctzll(cs) + 1;
    int stc = to < c->num_sec ? to : c->num_sec;

    /* compaction range */
    int nc = comp_nom_cap(c);
    int ncomp = (nc / 2) + (c->num_sec - stc) * c->sec_sz;
    if ((s0 - ncomp) & 1) ncomp--;
    int cs0, ce;
    if (c->hra) { cs0 = 0;     ce = s0 - ncomp; }
    else        { cs0 = ncomp; ce = s0; }
    if (ce - cs0 < 2) {
        /* Degenerate compaction range — nothing to compact. */
        c->state++;
        comp_ensure_sections(c);
        int nc1 = comp_nom_cap(c);
        compact_ret_t ret = { 0, nc1 - nc0, { NULL, 0, 0, 0, 1, 0 } };
        return ret;
    }

    int coin;
    if (c->state & 1) coin = !c->last_flip;
    else               coin = (int)(xo_next(rng) & 1);
    c->last_flip = coin;

    buf_t promoted = buf_evens_or_odds(&c->buf, cs0, ce, coin);
    buf_trim(&c->buf, s0 - (ce - cs0));
    c->state++;
    comp_ensure_sections(c);

    int s1 = c->buf.count;
    int nc1 = comp_nom_cap(c);
    compact_ret_t ret = { s1 - s0 + promoted.count, nc1 - nc0, promoted };
    return ret;
}

/* Merge other compactor's buffer into this one. */
static void comp_merge(compactor_t *dst, const compactor_t *src, uint64_t rng[4]) {
    assert(dst->lg_wt == src->lg_wt);
    dst->state |= src->state;
    while (comp_ensure_sections(dst)) {}

    buf_sort(&dst->buf);
    /* We must cast away const because buf_sort may reorder src's data.
       The source sketch is logically consumed by merge. */
    buf_sort((buf_t *)&src->buf);

    if (src->buf.count > dst->buf.count) {
        buf_t cp = buf_copy(&src->buf);
        buf_merge_in(&cp, &dst->buf);
        buf_free(&dst->buf);
        dst->buf = cp;
    } else {
        buf_merge_in(&dst->buf, (buf_t *)&src->buf);
    }
}

/* ── Sketch ────────────────────────────────────────────────────────── */
struct req_sketch {
    compactor_t *comps;
    int          ncomp;
    int          comp_cap;
    uint32_t     k;
    int          hra;        /* 1=HRA, 0=LRA */
    int          crit;       /* 0=LT, 1=LE */
    uint64_t     total_n;
    double       min_val;
    double       max_val;
    double       sum_val;
    int          retained;
    int          max_nom;
    uint64_t     rng[4];
};

static int sk_compute_max_nom(const req_sketch_t *sk) {
    int t = 0;
    for (int i = 0; i < sk->ncomp; i++) t += comp_nom_cap(&sk->comps[i]);
    return t;
}

static void sk_grow(req_sketch_t *sk) {
    if (sk->ncomp >= sk->comp_cap) {
        sk->comp_cap *= 2;
        sk->comps = (compactor_t *)realloc(sk->comps, sk->comp_cap * sizeof(compactor_t));
    }
    comp_init(&sk->comps[sk->ncomp], (uint8_t)sk->ncomp, sk->hra, sk->k);
    sk->ncomp++;
    sk->max_nom = sk_compute_max_nom(sk);
}

static void sk_compress(req_sketch_t *sk) {
    int h = 0;
    while (h < sk->ncomp) {
        compactor_t *c = &sk->comps[h];
        if (c->buf.count >= comp_nom_cap(c)) {
            if (h + 1 >= sk->ncomp) sk_grow(sk);
            c = &sk->comps[h]; /* re-fetch: sk_grow may realloc sk->comps */
            compact_ret_t cr = comp_compact(c, sk->rng);
            if (cr.promoted.data && cr.promoted.count > 0)
                buf_merge_in(&sk->comps[h + 1].buf, &cr.promoted);
            if (cr.promoted.data) buf_free(&cr.promoted);
            sk->retained += cr.dri;
            sk->max_nom  += cr.dns;
        }
        h++;
    }
}

/* ── Public API ────────────────────────────────────────────────────── */

req_sketch_t *req_new(uint32_t k, int rank_accuracy, uint64_t seed) {
    req_sketch_t *sk = (req_sketch_t *)calloc(1, sizeof(req_sketch_t));
    sk->k        = k;
    sk->hra      = rank_accuracy;
    sk->crit     = 0;
    sk->min_val  = NAN;
    sk->max_val  = NAN;
    sk->comp_cap = 8;
    sk->comps    = (compactor_t *)malloc(8 * sizeof(compactor_t));
    xo_seed(sk->rng, seed);
    sk_grow(sk);
    return sk;
}

void req_free(req_sketch_t *sk) {
    if (!sk) return;
    for (int i = 0; i < sk->ncomp; i++) comp_free(&sk->comps[i]);
    free(sk->comps);
    free(sk);
}

void req_insert(req_sketch_t *sk, double val) {
    if (__builtin_expect(val != val, 0)) return;

    if (__builtin_expect(sk->total_n == 0, 0)) {
        sk->min_val = val; sk->max_val = val;
    } else {
        if (val < sk->min_val) sk->min_val = val;
        if (val > sk->max_val) sk->max_val = val;
    }

    buf_append(&sk->comps[0].buf, val);
    sk->retained++;
    sk->total_n++;
    sk->sum_val += val;

    if (__builtin_expect(sk->retained >= sk->max_nom, 0)) {
        buf_sort(&sk->comps[0].buf);
        sk_compress(sk);
    }
}

void req_insert_batch(req_sketch_t *sk, const double *vals, int n) {
    for (int i = 0; i < n; i++)
        req_insert(sk, vals[i]);
}

uint64_t req_count(const req_sketch_t *sk)          { return sk->total_n; }
int      req_is_empty(const req_sketch_t *sk)       { return sk->total_n == 0; }
double   req_min(const req_sketch_t *sk)             { return sk->min_val; }
double   req_max(const req_sketch_t *sk)             { return sk->max_val; }
double   req_sum(const req_sketch_t *sk)             { return sk->sum_val; }
int      req_retained(const req_sketch_t *sk)        { return sk->retained; }
uint32_t req_k(const req_sketch_t *sk)               { return sk->k; }
int      req_rank_accuracy(const req_sketch_t *sk)   { return sk->hra; }
int      req_num_levels(const req_sketch_t *sk)      { return sk->ncomp; }
int      req_criterion(const req_sketch_t *sk)       { return sk->crit; }
void     req_set_criterion(req_sketch_t *sk, int c)  { sk->crit = c; }

uint64_t req_count_with_criterion(req_sketch_t *sk, double value) {
    if (sk->total_n == 0) return 0;
    uint64_t acc = 0;
    for (int i = 0; i < sk->ncomp; i++) {
        uint64_t wt = 1ULL << sk->comps[i].lg_wt;
        int cnt = buf_count_crit(&sk->comps[i].buf, value, sk->crit);
        acc += wt * (uint64_t)cnt;
    }
    return acc;
}

double req_rank(req_sketch_t *sk, double value) {
    if (sk->total_n == 0) return NAN;
    return (double)req_count_with_criterion(sk, value) / (double)sk->total_n;
}

/* ── Quantile query (builds ephemeral auxiliary) ───────────────────── */
typedef struct { double val; uint64_t wt; } witem_t;

static int cmp_witem(const void *a, const void *b) {
    double da = ((const witem_t *)a)->val, db = ((const witem_t *)b)->val;
    return (da > db) - (da < db);
}

double req_quantile(req_sketch_t *sk, double nr) {
    if (sk->total_n == 0) return NAN;

    /* Collect weighted items from all compactors */
    witem_t *wi = (witem_t *)malloc(sk->retained * sizeof(witem_t));
    int n = 0;
    for (int i = 0; i < sk->ncomp; i++) {
        compactor_t *c = &sk->comps[i];
        buf_sort(&c->buf);
        uint64_t wt = 1ULL << c->lg_wt;
        int lo, hi;
        if (c->buf.sab) { lo = c->buf.capacity - c->buf.count; hi = c->buf.capacity; }
        else             { lo = 0; hi = c->buf.count; }
        for (int j = lo; j < hi; j++) {
            wi[n].val = c->buf.data[j]; wi[n].wt = wt; n++;
        }
    }

    qsort(wi, n, sizeof(witem_t), cmp_witem);

    /* Cumulative weights */
    for (int i = 1; i < n; i++) wi[i].wt += wi[i - 1].wt;

    /* Dedup: keep last of consecutive equal values */
    int dn = 0;
    for (int i = 0; i < n; i++) {
        int j = i;
        while (j + 1 < n && wi[j + 1].val == wi[i].val) j++;
        wi[dn++] = wi[j];
        i = j;
    }

    uint64_t target = (uint64_t)(nr * (double)sk->total_n);

    /* Binary search based on criterion */
    int left = 0, right = dn;
    if (sk->crit == 0) {
        /* LT criterion → search for first cumwt > target (equivalent to IS.:>) */
        while (left < right) { int m = left + (right - left) / 2; if (wi[m].wt > target) right = m; else left = m + 1; }
    } else {
        /* LE criterion → search for first cumwt >= target (equivalent to IS.:>=) */
        while (left < right) { int m = left + (right - left) / 2; if (wi[m].wt >= target) right = m; else left = m + 1; }
    }
    if (left >= dn) left = dn - 1;

    double result = wi[left].val;
    free(wi);
    return result;
}

/* ── Merge ─────────────────────────────────────────────────────────── */
void req_merge(req_sketch_t *dst, const req_sketch_t *src) {
    if (src->total_n == 0) return;

    dst->total_n += src->total_n;
    if (dst->total_n - src->total_n == 0) {
        dst->min_val = src->min_val; dst->max_val = src->max_val;
    } else {
        if (src->min_val < dst->min_val) dst->min_val = src->min_val;
        if (src->max_val > dst->max_val) dst->max_val = src->max_val;
    }

    while (dst->ncomp < src->ncomp) sk_grow(dst);

    for (int i = 0; i < src->ncomp; i++)
        comp_merge(&dst->comps[i], &src->comps[i], dst->rng);

    dst->max_nom  = sk_compute_max_nom(dst);
    int total_ret = 0;
    for (int i = 0; i < dst->ncomp; i++) total_ret += dst->comps[i].buf.count;
    dst->retained = total_ret;

    if (dst->retained >= dst->max_nom) sk_compress(dst);
}
