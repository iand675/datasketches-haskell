-- | HyperLogLog sketch for cardinality (distinct count) estimation.
--
-- HyperLogLog estimates the number of distinct items in a data stream using
-- very little memory. The algorithm works by hashing each item and tracking
-- the maximum number of leading zeros observed in different hash partitions.
--
-- Accuracy is controlled by the precision parameter p, which determines
-- the number of registers (2^p). The standard error is approximately
-- 1.04 / sqrt(2^p).
--
-- Common configurations:
--
-- * p=10: 1024 registers (1KB), ~3.25% error
-- * p=12: 4096 registers (4KB), ~1.63% error
-- * p=14: 16384 registers (16KB), ~0.81% error
-- * p=16: 65536 registers (64KB), ~0.41% error
--
-- Items must be represented as Word64 values. Apply a hash function to convert
-- your domain types before inserting.
--
-- Sketches are fully mergeable: the union of two HLL sketches gives the same
-- result as inserting all items from both streams into a single sketch.
module DataSketches.Distinct.HyperLogLog
  ( -- * Construction
    HllSketch
  , mkHllSketch
  -- * Updating
  , insert
  , merge
  -- * Querying
  , estimate
  , precision
  ) where

import Control.Monad.Primitive (PrimMonad(PrimState))
import Data.Word (Word64)
import DataSketches.Distinct.HyperLogLog.Internal

-- | Insert an item into the sketch. The item should be a hash of the original value.
insert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
insert = hllInsert

-- | Estimate the number of distinct items inserted.
estimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
estimate = hllEstimate

-- | Merge the second sketch into the first. Both must have the same precision.
merge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
merge = hllMerge

-- | Get the precision (log2 of register count) of the sketch.
precision :: HllSketch s -> Int
precision = hllPrecision
