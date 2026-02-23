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
| REQ insert 100 (k=6) | 1,943 | 601 | 3.2x |
| REQ insert 1,000 (k=6) | 4,145 | 159 | 26x |
| REQ insert 10,000 (k=6) | 3,101 | 99 | 31x |
| REQ insert 100,000 (k=6) | 2,908 | 34 | 86x |
| **KLL insert 100 (k=200)** | **26** | **344** | **0.08x (13x faster)** |
| **KLL insert 1,000 (k=200)** | **34** | **123** | **0.28x (3.6x faster)** |
| KLL insert 10,000 (k=200) | 63 | 31 | 2.0x |
| KLL insert 100,000 (k=200) | 76 | 18 | 4.2x |

### Quantile Sketches — Rank Query

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| REQ rank (100 queries, 10k items) | 7,133 | 349 | 20x |
| **KLL rank (100 queries, 10k items)** | **208** | **531** | **0.39x (2.6x faster)** |

### Distinct Counting / Frequency

| Benchmark | Haskell (ns/op) | Java (ns/op) | Ratio |
|-----------|----------------:|-------------:|------:|
| HLL insert 1,000 (p=12) | 21 | 161 | **0.13x (8x faster)** |
| HLL insert 10,000 (p=12) | 38 | 24 | 1.6x |
| HLL insert 100,000 (p=12) | 35 | 12 | 2.9x |
| CountMin insert 10,000 | 24 | — | — |
| CountMin insert 100,000 | 35 | — | — |

**Notes:**
- KLL, HLL, CountMin, and Theta are implemented as C structs with all operations in C (`cbits/`). The Haskell side is a thin `ForeignPtr` wrapper — zero GC pressure, zero monadic bind overhead. REQ remains in Haskell (most complex algorithm with multiple interacting mutable structures).
- KLL beats Java at small-to-medium N due to C's direct pointer arithmetic vs JVM's object model overhead. At large N, Java's JIT adaptive optimization catches up on the tight sort/compact inner loops.
- HLL beats Java at small N (8x faster at 1k). At large N the JVM's hash intrinsics give Java the edge.

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
