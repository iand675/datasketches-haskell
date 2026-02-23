# DataSketches

[![Build status](https://badge.buildkite.com/7b2af8b4b0c08a5db141959addf751380574ac8079a7d8cd00.svg)](https://buildkite.com/iand675/datasketches)

A Haskell port of the [Apache DataSketches](https://datasketches.apache.org/) library — stochastic streaming algorithms for approximate computation on large datasets.

## Sketch Families

| Sketch | Module | Purpose |
|--------|--------|---------|
| REQ (Relative Error Quantiles) | `DataSketches.Quantiles.RelativeErrorQuantile` | Quantiles with relative error — extremely accurate at distribution tails (99.999th or 0.001th percentile) |
| KLL | `DataSketches.Quantiles.KLL` | Quantiles with additive error bounds — uniform ~1.3% error at k=200 |
| HyperLogLog | `DataSketches.Distinct.HyperLogLog` | Cardinality (distinct count) estimation — ~1.6% error with 4KB |
| Theta | `DataSketches.Distinct.Theta` | Distinct counting with set operations (union, intersection, difference) |
| Count-Min | `DataSketches.Frequencies.CountMin` | Frequency estimation — may overcount, never undercounts |

## Quick Start

```haskell
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ

main :: IO ()
main = do
  sk <- REQ.mkReqSketch 12 REQ.HighRanksAreAccurate
  mapM_ (REQ.insert sk) [1..10000 :: Double]
  p99 <- REQ.quantile sk 0.99
  putStrLn $ "p99 = " ++ show p99
```

## Benchmarks

Insert throughput (ns/op) compared against the Java DataSketches 6.1.1 reference.
Lower is better. Measured on the same machine, single-threaded.

### Quantile Sketches — Insert

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| REQ insert 100 (k=6) | 1,876 | 593 | 3.2x |
| REQ insert 1,000 (k=6) | 3,982 | 163 | 24x |
| REQ insert 10,000 (k=6) | 3,188 | 89 | 36x |
| REQ insert 100,000 (k=6) | 3,155 | 33 | 96x |
| KLL insert 100 (k=200) | 172 | 322 | **0.53x** |
| KLL insert 1,000 (k=200) | 702 | 124 | 5.7x |
| KLL insert 10,000 (k=200) | 959 | 31 | 31x |
| KLL insert 100,000 (k=200) | 954 | 18 | 53x |

### Quantile Sketches — Rank Query

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| REQ rank (100 queries, 10k items) | 6,968 | 431 | 16x |
| KLL rank (100 queries, 10k items) | 76,490 | 352 | 217x |

### Distinct Counting / Frequency

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| HLL insert 1,000 (p=12) | 19 | 121 | **0.16x** |
| HLL insert 10,000 (p=12) | 25 | 16 | 1.6x |
| HLL insert 100,000 (p=12) | 20 | 7 | 2.9x |
| CountMin insert 10,000 | 46 | — | — |
| CountMin insert 100,000 | 31 | — | — |

**Notes:**
- KLL uses flat contiguous unboxed storage (single `ByteArray#` for all items, matching Java's `double[]` layout). Level 0 grows leftward for O(1) insert with no shifting.
- HLL insert is pure Haskell on a raw `MutableByteArray` (same representation as C `uint8_t[]`). HLL estimate uses a C function with `ldexp()` — the pure Haskell version was using `(^^)` which compiled to Integer-based exponentiation.
- CountMin insert is pure Haskell on raw `MutableByteArray`. CountMin estimate uses a C function for the min-reduction across hash rows.
- A C `xoshiro256++` RNG and `sort_doubles` (insertion sort for small arrays, qsort for large) are available in cbits for future use.
- Haskell REQ uses `Double` (64-bit); Java REQ uses `float` (32-bit).
- The KLL/REQ rank query cost reflects materializing weighted items on each query. The Java library maintains pre-sorted auxiliary structures.

### Running Benchmarks

```bash
# Haskell
stack bench

# Java (requires JDK)
cd java-harness
javac -cp "lib/*" SketchBench.java
java -cp ".:lib/*" SketchBench
```

## Cross-Validation

The Haskell implementations are cross-validated against the Java DataSketches 6.1.1 library using Hedgehog property-based testing (700 test cases). In exact mode (no compaction), results match exactly. In estimation mode, both implementations are independently verified against ground truth.

```bash
# Compile Java harness (one-time)
cd java-harness && javac -cp "lib/*" SketchHarness.java

# Run all tests including cross-validation
stack test
```

## Packages

- **data-sketches-core**: Internal implementations and data structures
- **data-sketches**: Public API with documentation
