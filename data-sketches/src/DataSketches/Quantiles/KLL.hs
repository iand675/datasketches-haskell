-- | KLL (Karnin-Lang-Liberty) sketch for approximate quantile computation
-- with additive error bounds. Based on "Optimal Quantile Approximation
-- in Streams" (Karnin, Lang, Liberty — FOCS 2016).
--
-- Feed it a stream of numbers, then ask questions like "what's the median?"
-- or "what value is at the 99th percentile?" — without storing every value.
-- The sketch keeps a small, fixed-size buffer regardless of how many items
-- you insert.
--
-- KLL provides uniform additive error across all ranks. With @k = 200@,
-- the normalized rank error is approximately 1.33% with 99% confidence,
-- regardless of whether you query p50 or p99.
--
-- This is the recommended quantile sketch for most use cases. Prefer
-- "DataSketches.Quantiles.RelativeErrorQuantile" only when you need
-- high accuracy specifically at the distribution tails (p99.9+).
--
-- === Implementation
--
-- Backed by a C implementation (@cbits\/kll.c@) behind a 'ForeignPtr'.
-- All sketch memory lives outside the GHC heap — zero GC pressure,
-- zero boxing overhead.
--
-- === Usage
--
-- @
-- import qualified DataSketches.Quantiles.KLL as KLL
--
-- main :: IO ()
-- main = do
--   sk <- KLL.'mkKllSketch' 200
--   mapM_ (KLL.'insert' sk) [1..100000 :: Double]
--   p99 <- KLL.'quantile' sk 0.99
--   putStrLn $ "p99 = " ++ show p99
-- @
--
-- === Mergeability
--
-- Fully mergeable: 'merge' combines two sketches as if all data had been
-- inserted into one. Suitable for parallel and distributed computation.
module DataSketches.Quantiles.KLL
  ( -- * Construction
    KllSketch
  , mkKllSketch
  -- * Updating the sketch
  , insert
  , insertBatch
  , merge
  -- * Querying
  , count
  , null
  , minimum
  , maximum
  , retainedItemCount
  , quantile
  , quantiles
  , rank
  , ranks
  , cumulativeDistributionFunction
  , probabilityMassFunction
  ) where

import Prelude hiding (null, minimum, maximum)
import Control.Monad (unless, when)
import Control.Monad.Primitive (PrimMonad(PrimState))
import qualified Data.Vector.Storable as VS
import Data.Word (Word32, Word64)
import DataSketches.Quantiles.KLL.Internal

-- | Create a new KLL sketch with the given accuracy parameter k.
-- Larger k gives better accuracy but uses more space.
-- k must be >= 8. Default recommendation is 200.

-- | Insert a value into the sketch. NaN values are ignored.
insert :: PrimMonad m => KllSketch (PrimState m) -> Double -> m ()
insert = kllInsert

-- | Bulk-insert a storable vector of doubles. Avoids per-element FFI overhead.
insertBatch :: PrimMonad m => KllSketch (PrimState m) -> VS.Vector Double -> m ()
insertBatch = kllInsertBatch

-- | Merge the second sketch into the first.
merge :: PrimMonad m => KllSketch (PrimState m) -> KllSketch (PrimState m) -> m ()
merge = kllMerge

-- | Total number of items inserted.
count :: PrimMonad m => KllSketch (PrimState m) -> m Word64
count = kllCount

-- | True if no items have been inserted.
null :: PrimMonad m => KllSketch (PrimState m) -> m Bool
null = kllIsEmpty

-- | Smallest value seen.
minimum :: PrimMonad m => KllSketch (PrimState m) -> m Double
minimum = kllMinimum

-- | Largest value seen.
maximum :: PrimMonad m => KllSketch (PrimState m) -> m Double
maximum = kllMaximum

-- | Number of items currently retained in the sketch.
retainedItemCount :: PrimMonad m => KllSketch (PrimState m) -> m Int
retainedItemCount = kllRetainedItems

-- | Get the approximate quantile for the given normalized rank in [0, 1].
quantile :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
quantile = kllQuantile

-- | Get approximate quantiles for multiple normalized ranks.
quantiles :: PrimMonad m => KllSketch (PrimState m) -> [Double] -> m [Double]
quantiles sk = mapM (quantile sk)

-- | Get the approximate normalized rank of a value.
rank :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
rank = kllRank

-- | Get approximate normalized ranks for multiple values.
ranks :: PrimMonad m => KllSketch (PrimState m) -> [Double] -> m [Double]
ranks sk = mapM (rank sk)

-- | Approximate cumulative distribution function.
-- Given a list of split points, returns a list of cumulative probabilities.
-- Returns Nothing if the sketch is empty.
cumulativeDistributionFunction
  :: PrimMonad m
  => KllSketch (PrimState m)
  -> [Double]
  -> m (Maybe [Double])
cumulativeDistributionFunction sk splitPoints = do
  empty <- null sk
  if empty
    then pure Nothing
    else do
      rs <- mapM (rank sk) splitPoints
      pure (Just (rs ++ [1.0]))

-- | Approximate probability mass function.
-- Given a list of split points, returns probabilities for each interval.
-- Returns empty list if the sketch is empty.
probabilityMassFunction
  :: PrimMonad m
  => KllSketch (PrimState m)
  -> [Double]
  -> m [Double]
probabilityMassFunction sk splitPoints = do
  empty <- null sk
  if empty
    then pure []
    else do
      rs <- mapM (rank sk) splitPoints
      let cdf = rs ++ [1.0]
          pmf = zipWith (-) cdf (0.0 : cdf)
      pure pmf
