-- | HyperLogLog sketch for cardinality (distinct count) estimation.
--
-- "How many /different/ things have I seen?" — not how many total, but how
-- many unique. Think unique visitors, unique IPs, unique search queries.
-- Counting exactly requires remembering every item you've seen (a set), which
-- grows with the data. HyperLogLog answers the same question using a few
-- kilobytes, no matter how large the stream.
--
-- It works by hashing each item and tracking the maximum number of leading
-- zeros observed across 2^p independent register buckets.
--
-- The standard error is approximately @1.04 / sqrt(2^p)@.
--
-- === Precision configurations
--
-- +------+-----------+------+-------------+
-- | @p@  | Registers | RAM  | Std. error  |
-- +======+===========+======+=============+
-- | 10   | 1,024     | 1 KB | ~3.25%      |
-- +------+-----------+------+-------------+
-- | 12   | 4,096     | 4 KB | ~1.63%      |
-- +------+-----------+------+-------------+
-- | 14   | 16,384    | 16 KB| ~0.81%      |
-- +------+-----------+------+-------------+
-- | 16   | 65,536    | 64 KB| ~0.41%      |
-- +------+-----------+------+-------------+
--
-- === Hashing
--
-- Items must be pre-hashed to 'Word64'. Apply a good hash function (e.g.
-- from @hashable@ or @xxhash@) to your domain types before calling 'insert'.
-- The quality of the cardinality estimate depends on the hash being uniform.
--
-- === Implementation
--
-- Backed by a C implementation (@cbits\/hll.c@) behind a 'ForeignPtr'.
-- All sketch memory lives outside the GHC heap.
--
-- === Usage
--
-- @
-- import qualified DataSketches.Distinct.HyperLogLog as HLL
-- import Data.Hashable (hash)
--
-- main :: IO ()
-- main = do
--   sk <- HLL.'mkHllSketch' 12
--   mapM_ (HLL.'insert' sk . fromIntegral . hash) [\"alice\", \"bob\", \"alice\"]
--   n <- HLL.'estimate' sk
--   putStrLn $ "distinct count ≈ " ++ show n  -- ≈ 2.0
-- @
--
-- === Mergeability
--
-- Fully mergeable via 'merge'. The union of two HLL sketches gives the same
-- result as inserting all items from both streams into a single sketch. Both
-- must share the same precision @p@.
--
-- === When to use Theta instead
--
-- HyperLogLog only supports cardinality estimation and union. If you need
-- set intersection or difference, use "DataSketches.Distinct.Theta".
module DataSketches.Distinct.HyperLogLog
  ( -- * Construction
    HllSketch
  , mkHllSketch
  -- * Updating
  , insert
  , insertBatch
  , merge
  -- * Querying
  , estimate
  , precision
  ) where

import Control.Monad.Primitive (PrimMonad, PrimState)
import qualified Data.Vector.Storable as VS
import Data.Word (Word64)
import DataSketches.Distinct.HyperLogLog.Internal

-- | Insert an item into the sketch. The item should be a hash of the original value.
insert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
insert = hllInsert

-- | Bulk-insert a storable vector of hashed items. Avoids per-element FFI overhead.
insertBatch :: PrimMonad m => HllSketch (PrimState m) -> VS.Vector Word64 -> m ()
insertBatch = hllInsertBatch

-- | Estimate the number of distinct items inserted.
estimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
estimate = hllEstimate

-- | Merge the second sketch into the first. Both must have the same precision.
merge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
merge = hllMerge

-- | Get the precision (log2 of register count) of the sketch.
precision :: PrimMonad m => HllSketch (PrimState m) -> m Int
precision = hllPrecision
