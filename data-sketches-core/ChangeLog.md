# Changelog for data-sketches-core

## 0.3.0.0 — 2026-04-08

### Breaking changes

- **Removed old pure-Haskell REQ internals**: The following modules have been
  removed. All sketch operations now go through the C backend (`CInternal`),
  which replaced these modules in 0.2.0.0 but left the dead code in the
  package. Downstream consumers should use the public `data-sketches` API.

  - `DataSketches.Core.Internal.URef`
  - `DataSketches.Core.Snapshot`
  - `DataSketches.Quantiles.RelativeErrorQuantile.Internal`
  - `DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary`
  - `DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor`
  - `DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer`
  - `DataSketches.Quantiles.RelativeErrorQuantile.Internal.InequalitySearch`

- **`Criterion` type removed from `Types` module**: The `Criterion` type and
  its `InequalitySearch` class instance were only used by the deleted Haskell
  internals. Criterion-based queries are handled entirely in C.

- **`DoubleIsNonFiniteException` moved to `Types` module**: Previously exported
  from `Internal.DoubleBuffer`, now exported from
  `DataSketches.Quantiles.RelativeErrorQuantile.Types`.

### Performance

- **HLL estimate: 2.6x faster** — replaced `ldexp(1.0, -val)` (libm call per
  register) with IEEE 754 bit manipulation (`pow2_neg`). Eliminates ~4096
  function calls per estimate at p=12.

- **NEON/SSE2 intrinsics for HLL and KLL** — explicit SIMD paths for
  `hll_c_merge` (16-wide `uint8` max via `vmaxq_u8`/`_mm_max_epu8`),
  `hll_c_estimate` zero-counting (16-wide via `vceqq_u8`/`_mm_cmpeq_epi8`),
  and `kll_rank` inner loop (2-4 wide `double` comparison via
  `vcltq_f64`/`_mm_cmplt_pd`). Scalar fallback on other architectures.

- Added `__restrict__` qualifiers to `cms_c_merge` to guarantee
  auto-vectorization of the bulk `uint64` addition.

### Dependency changes

- Dropped `mwc-random` and `vector-algorithms` dependencies (only needed by
  the removed Haskell internals).

## 0.2.0.1

### Bug fixes

- **Missing `include-dirs` for C headers**: `cbits/req.c` includes `req.h` but the
  cabal file had no `include-dirs` entry pointing at `cbits/`, causing a build failure
  (`fatal error: req.h: No such file or directory`). Added `include-dirs: cbits` and
  listed header files in `extra-source-files` so they're included in sdist tarballs.

## 0.2.0.0

### New sketch families

- **KLL Sketch** (`DataSketches.Quantiles.KLL.Internal`): Quantiles sketch with
  additive error bounds. Implemented entirely in C for performance — 26-41x faster
  than the initial Haskell version, faster than Java DataSketches at small-to-medium N.

- **HyperLogLog** (`DataSketches.Distinct.HyperLogLog.Internal`): Cardinality
  estimation using hash-based registers and harmonic mean. C implementation with
  `ldexp`-based estimate (replacing the Haskell `(^^)` which compiled to Integer
  exponentiation).

- **Theta Sketch** (`DataSketches.Distinct.Theta.Internal`): Distinct counting with
  set operations (union, intersection, difference). C implementation with sorted
  entries for O(log k) duplicate checks.

- **Count-Min Sketch** (`DataSketches.Frequencies.CountMin.Internal`): Frequency
  estimation. C implementation with Lemire fast modular reduction (128-bit
  widening multiply instead of 64-bit division).

### Bug fixes

- **Off-by-one in `getCountWithCriterion`**: When `spaceAtBottom=False`, the binary
  search upper bound was `count_` instead of `count_ - 1`, causing reads past the
  active buffer region into uninitialized memory. This produced intermittent
  incorrect rank calculations in `LowRanksAreAccurate` mode.

### Performance

- **C implementations** (`cbits/`): KLL, HLL, CountMin, and Theta sketches are
  implemented as C structs with all operations in C. Zero GC pressure, zero monadic
  bind overhead, zero dictionary passing.

- **Packed mutable fields**: REQ sketch's `ReqSketch`, `ReqCompactor`, and
  `DoubleBuffer` pack all mutable scalar fields into single `MutableByteArray`s
  sized to fit within one 64-byte cache line.

- **INLINE pragmas**: Added to all URef operations, DoubleBuffer field accessors,
  and hot-path functions to eliminate dictionary passing overhead.

- **C optimization details**:
  - Cached `total_capacity` in KLL struct (avoids recomputing `pow(2/3, depth)` per insert)
  - Insertion sort for small arrays (≤32 elements) in KLL compaction
  - Branchless register update and merge in HLL
  - Lemire fast modular reduction in CountMin (128-bit multiply, ~4 cycles vs ~30 for division)
  - Binary search for duplicate detection in Theta (O(log k) vs O(k))
  - xoshiro256++ RNG (32 bytes state vs mwc-random's 1032 bytes)

## 0.1.0.0

- Initial release with REQ (Relative Error Quantiles) sketch.
