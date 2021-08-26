# DataSketeches

[![Build status](https://badge.buildkite.com/7b2af8b4b0c08a5db141959addf751380574ac8079a7d8cd00.svg)](https://buildkite.com/iand675/datasketches)
[![data-sketches on stackage](https://stackage.org/package/packagename/badge/nightly)](https://stackage.org/nightly/package/data-sketches)
[![data-sketches on hackage](https://img.shields.io/hackage/v/data-sketches)](https://hackage.haskell.org/package/data-sketches)

A port of subsets of the [Apache DataSketches](https://datasketches.apache.org/) library.

## Overview

In the analysis of big data there are often problem queries that don’t scale because they require huge compute resources and time to generate exact results. Examples include count distinct, quantiles, most-frequent items, joins, matrix computations, and graph analysis.

Stocastic streaming algorithms for operating on large datasets. If approximate results are acceptable, there is a class of specialized algorithms, called streaming algorithms, or sketches that can produce results orders-of magnitude faster and with mathematically proven error bounds. For interactive queries there may not be other viable alternatives, and in the case of real-time analysis, sketches are the only known solution.

