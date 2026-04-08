# Changelog for data-sketches

## 0.4.0.1 — 2026-04-08

### Internal cleanup

- Removed dead pure-Haskell REQ internals that were superseded by the C
  backend in 0.4.0.0. No public API changes — `insert`, `quantile`, `rank`,
  `countWithCriterion`, and all other exports continue to work identically.

- Dropped unused `mwc-random`, `vector-algorithms`, `mtl`, and
  `hspec-junit-formatter` dependencies.

### Performance

- HLL `estimate` is ~2.6x faster (C backend: `ldexp` replaced with bit
  manipulation; NEON/SSE2 intrinsics for merge and zero-counting).

- Added estimate, merge, and query benchmarks for HLL, KLL, and Count-Min
  sketches (criterion suite).

### Test improvements

- Added `countWithCriterion` coverage: LT/LE threshold counting, boundary
  behavior at min/max, duplicate values, estimation mode, and non-finite
  exception handling.

- Added quantile monotonicity tests: verifies `quantile(r1) <= quantile(r2)`
  for `r1 <= r2` in exact mode, HRA estimation mode, and LRA estimation mode.

- Added estimation mode capacity growth tests: 10K inserts with accuracy
  checks; batch vs sequential insert equivalence.

## 0.4.0.0

### New sketch families

- **KLL Sketch** (`DataSketches.Quantiles.KLL`): Quantiles with uniform additive
  error bounds (~1.3% at k=200). C backend, faster than Java DataSketches at
  small-to-medium N.

- **HyperLogLog** (`DataSketches.Distinct.HyperLogLog`): Cardinality estimation.
  ~1.6% error with 4KB (p=12). C backend.

- **Theta Sketch** (`DataSketches.Distinct.Theta`): Distinct counting with set
  operations — `union`, `intersection`, `difference`. C backend.

- **Count-Min Sketch** (`DataSketches.Frequencies.CountMin`): Frequency estimation
  that may overcount but never undercounts. C backend.

### Bug fixes in REQ Sketch

- **Off-by-one in `getCountWithCriterion`**: Binary search read past the active
  buffer region when `spaceAtBottom=False` (`LowRanksAreAccurate` mode), causing
  intermittent incorrect rank calculations from uninitialized memory.

- **`growUntil` in `merge` didn't loop**: Only added one compactor level instead of
  recursing to the target. Silently dropped data from higher-level compactors when
  merging sketches with very different level counts.

- **Max value comparison inverted in `merge`**: `otherMax < thisMax` instead of
  `otherMax > thisMax` caused the sketch to track the wrong maximum after merging.

- **`compress` captured compactors vector once**: Replaced `imapM_` over a snapshot
  with a loop that re-reads the vector each iteration, matching the Java
  implementation's dynamic size check.

### Cross-validation

- Hedgehog property-based tests comparing Haskell outputs against Java DataSketches
  6.1.1 reference. Exact equality in exact mode (no compaction); independent
  ground-truth validation in estimation mode.

### Performance

- REQ insert: ~3x slower than Java (Haskell implementation with packed fields)
- KLL insert/100: **13x faster than Java** (C implementation)
- KLL rank queries: **2.6x faster than Java**
- HLL insert/1k: **8x faster than Java**

## 0.3.1.0

- Initial public API for REQ sketch.

## 0.3.0.0

- Internal refactoring.
