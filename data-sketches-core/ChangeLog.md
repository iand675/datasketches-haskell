# Changelog for data-sketches-core

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
