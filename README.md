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
| REQ insert 100 (k=6) | 1,879 | 593 | 3.2x |
| REQ insert 1,000 (k=6) | 3,720 | 163 | 23x |
| REQ insert 10,000 (k=6) | 2,897 | 89 | 33x |
| REQ insert 100,000 (k=6) | 2,490 | 33 | 75x |
| KLL insert 100 (k=200) | 171 | 322 | **0.53x** |
| KLL insert 1,000 (k=200) | 650 | 124 | 5.2x |
| KLL insert 10,000 (k=200) | 902 | 31 | 29x |
| KLL insert 100,000 (k=200) | 982 | 18 | 55x |

### Quantile Sketches — Rank Query

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| REQ rank (100 queries, 10k items) | 7,912 | 431 | 18x |
| KLL rank (100 queries, 10k items) | 71,600 | 352 | 203x |

### Distinct Counting / Frequency

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| HLL insert 1,000 (p=12) | 18 | 121 | **0.15x** |
| HLL insert 10,000 (p=12) | 21 | 16 | 1.3x |
| HLL insert 100,000 (p=12) | 18 | 7 | 2.6x |
| CountMin insert 10,000 | 38 | — | — |
| CountMin insert 100,000 | 41 | — | — |

**Notes:**
- KLL uses flat contiguous unboxed storage (single `ByteArray#` for all items, matching Java's `double[]` layout). Level 0 grows leftward for O(1) insert with no shifting.
- HLL and CountMin insert are very competitive with Java because they're simple hash + update operations with no sorting or compaction.
- Haskell REQ uses `Double` (64-bit); Java REQ uses `float` (32-bit). The Haskell version does more work per item for the same k due to wider values and the sort-heavy compaction path.
- The KLL/REQ rank query cost reflects materializing weighted items on each query. The Java library maintains pre-sorted auxiliary structures.
- The Java library has had extensive optimization over many years. The Haskell port prioritizes correctness and idiomatic code. The main remaining overheads are dictionary passing (polymorphic `PrimMonad m`), the REQ compactors stored as a boxed `Vector` of mutable records, and GC pressure from the sort-compact cycle.

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
