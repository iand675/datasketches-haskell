#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/*
 * Shared utility functions used by the Haskell FFI layer.
 * The sketch-specific implementations are in kll.c, hll.c, countmin.c, theta.c.
 */

/* ========================================================================
 * xoshiro256++ fast PRNG (Blackman & Vigna, public domain)
 * ======================================================================== */

static inline uint64_t rotl64(uint64_t x, int k) {
    return (x << k) | (x >> (64 - k));
}

uint64_t xoshiro256pp_next(uint64_t state[4]) {
    uint64_t result = rotl64(state[0] + state[3], 23) + state[0];
    uint64_t t = state[1] << 17;
    state[2] ^= state[0]; state[3] ^= state[1];
    state[1] ^= state[2]; state[0] ^= state[3];
    state[2] ^= t; state[3] = rotl64(state[3], 45);
    return result;
}

int xoshiro256pp_coin(uint64_t state[4]) {
    return (int)(xoshiro256pp_next(state) & 1);
}

void xoshiro256pp_seed(uint64_t state[4], uint64_t seed) {
    for (int i = 0; i < 4; i++) {
        seed += 0x9E3779B97F4A7C15ULL;
        uint64_t z = seed;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        state[i] = z ^ (z >> 31);
    }
}
