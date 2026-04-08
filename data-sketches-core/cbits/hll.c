#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define HLL_HAS_NEON 1
#elif defined(__SSE2__)
#include <emmintrin.h>
#define HLL_HAS_SSE2 1
#endif

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

/* 2^(-val) via IEEE 754 bit manipulation.
   For val in [0, 1022]: exponent = 1023 - val, mantissa = 0.
   For val >= 1023 (shouldn't happen with HLL registers ≤ 64): returns 0.0. */
static inline double pow2_neg(int val) {
    if (__builtin_expect(val > 1022, 0)) return 0.0;
    union { uint64_t u; double d; } bits;
    bits.u = (uint64_t)(1023 - val) << 52;
    return bits.d;
}

double hll_c_estimate(const hll_sketch_t *sk) {
    int m = sk->m;
    double mf = (double)m;
    double harmonic_sum = 0.0;
    int zero_count = 0;
    const uint8_t *regs = sk->registers;

#if HLL_HAS_NEON
    /* NEON: count zeros 16 registers at a time */
    {
        uint8x16_t zero_vec = vdupq_n_u8(0);
        uint8x16_t zcount16 = vdupq_n_u8(0);
        int i = 0;
        int chunks = m & ~15;
        int batch = 0;
        for (; i < chunks; i += 16) {
            uint8x16_t data = vld1q_u8(regs + i);
            uint8x16_t eq = vceqq_u8(data, zero_vec);
            /* eq lanes are 0xFF where zero, 0x00 otherwise.
               Subtracting 0xFF is adding 1 in unsigned wrapping. */
            zcount16 = vsubq_u8(zcount16, eq);
            batch++;
            /* Flush to avoid uint8 overflow (max 255 accumulated) */
            if (batch == 255) {
                uint16x8_t sum16 = vpaddlq_u8(zcount16);
                uint32x4_t sum32 = vpaddlq_u16(sum16);
                uint64x2_t sum64 = vpaddlq_u32(sum32);
                zero_count += (int)(vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1));
                zcount16 = vdupq_n_u8(0);
                batch = 0;
            }
        }
        /* Flush remaining accumulated zeros */
        {
            uint16x8_t sum16 = vpaddlq_u8(zcount16);
            uint32x4_t sum32 = vpaddlq_u16(sum16);
            uint64x2_t sum64 = vpaddlq_u32(sum32);
            zero_count += (int)(vgetq_lane_u64(sum64, 0) + vgetq_lane_u64(sum64, 1));
        }
        /* Scalar tail */
        for (; i < m; i++)
            zero_count += (regs[i] == 0);
    }
    /* Harmonic sum (scalar with pow2_neg — hard to vectorize the
       uint8→double widening chain profitably) */
    for (int i = 0; i < m; i++)
        harmonic_sum += pow2_neg((int)regs[i]);
#elif HLL_HAS_SSE2
    /* SSE2: count zeros 16 registers at a time */
    {
        __m128i zero_vec = _mm_setzero_si128();
        __m128i zcount16 = _mm_setzero_si128();
        int i = 0;
        int chunks = m & ~15;
        int batch = 0;
        for (; i < chunks; i += 16) {
            __m128i data = _mm_loadu_si128((const __m128i *)(regs + i));
            __m128i eq = _mm_cmpeq_epi8(data, zero_vec);
            zcount16 = _mm_sub_epi8(zcount16, eq);
            batch++;
            if (batch == 255) {
                /* Horizontal sum via SAD against zero */
                __m128i sad = _mm_sad_epu8(zcount16, _mm_setzero_si128());
                zero_count += _mm_extract_epi16(sad, 0) + _mm_extract_epi16(sad, 4);
                zcount16 = _mm_setzero_si128();
                batch = 0;
            }
        }
        {
            __m128i sad = _mm_sad_epu8(zcount16, _mm_setzero_si128());
            zero_count += _mm_extract_epi16(sad, 0) + _mm_extract_epi16(sad, 4);
        }
        for (; i < m; i++)
            zero_count += (regs[i] == 0);
    }
    for (int i = 0; i < m; i++)
        harmonic_sum += pow2_neg((int)regs[i]);
#else
    for (int i = 0; i < m; i++) {
        int val = (int)regs[i];
        harmonic_sum += pow2_neg(val);
        zero_count += (val == 0);
    }
#endif

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

void hll_c_merge(hll_sketch_t *dst, const hll_sketch_t *src) {
    int m = dst->m;
    uint8_t *__restrict__ d = dst->registers;
    const uint8_t *__restrict__ s = src->registers;

#if HLL_HAS_NEON
    int i = 0;
    int chunks = m & ~15;
    for (; i < chunks; i += 16) {
        uint8x16_t dv = vld1q_u8(d + i);
        uint8x16_t sv = vld1q_u8(s + i);
        vst1q_u8(d + i, vmaxq_u8(dv, sv));
    }
    for (; i < m; i++) {
        uint8_t sv = s[i], dv = d[i];
        d[i] = sv > dv ? sv : dv;
    }
#elif HLL_HAS_SSE2
    int i = 0;
    int chunks = m & ~15;
    for (; i < chunks; i += 16) {
        __m128i dv = _mm_loadu_si128((const __m128i *)(d + i));
        __m128i sv = _mm_loadu_si128((const __m128i *)(s + i));
        _mm_storeu_si128((__m128i *)(d + i), _mm_max_epu8(dv, sv));
    }
    for (; i < m; i++) {
        uint8_t sv = s[i], dv = d[i];
        d[i] = sv > dv ? sv : dv;
    }
#else
    for (int i = 0; i < m; i++) {
        uint8_t sv = s[i], dv = d[i];
        d[i] = sv > dv ? sv : dv;
    }
#endif
}

int hll_c_precision(const hll_sketch_t *sk) { return sk->p; }
