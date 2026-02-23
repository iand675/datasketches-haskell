-- | A Count-Min Sketch for approximate frequency estimation of items in a stream.
--
-- The Count-Min Sketch uses sub-linear space to estimate how many times each item
-- has appeared. It may overcount (due to hash collisions) but never undercounts.
--
-- The accuracy is controlled by two parameters:
--
-- * epsilon (ε): error tolerance. The estimate will be within ε*N of the true count
--   with high probability, where N is the total number of items inserted.
--
-- * delta (δ): failure probability. The probability that the estimate exceeds the
--   error bound is at most δ.
--
-- Space used is O(1/ε * log(1/δ)).
--
-- Items are represented as Word64 values. Use a hash function to convert
-- your domain-specific items to Word64 before inserting.
module DataSketches.Frequencies.CountMin
  ( -- * Construction
    CountMinSketch
  , mkCountMinSketch
  -- * Updating
  , insert
  , insertN
  , merge
  -- * Querying
  , estimate
  , width
  , depth
  ) where

import Control.Monad.Primitive (PrimMonad(PrimState))
import Data.Word (Word64)
import DataSketches.Frequencies.CountMin.Internal

-- | Insert a single occurrence of an item.
insert :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m ()
insert = cmsInsert

-- | Insert multiple occurrences of an item.
insertN :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> Word64 -> m ()
insertN = cmsInsertN

-- | Estimate the frequency of an item. May overcount but never undercounts.
estimate :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m Word64
estimate = cmsEstimate

-- | Merge the second sketch into the first. Both must have the same dimensions
-- (same epsilon and delta parameters at construction).
merge :: PrimMonad m => CountMinSketch (PrimState m) -> CountMinSketch (PrimState m) -> m ()
merge = cmsMerge

-- | Number of columns in the sketch.
width :: CountMinSketch s -> Int
width = cmsWidth

-- | Number of rows (hash functions) in the sketch.
depth :: CountMinSketch s -> Int
depth = cmsDepth
