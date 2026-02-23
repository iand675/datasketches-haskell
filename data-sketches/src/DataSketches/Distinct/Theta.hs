-- | Theta Sketch for distinct counting with set operations.
--
-- The Theta Sketch is a distinct-counting sketch that also supports set operations:
-- union, intersection, and set difference (A-not-B). This makes it useful for
-- computing metrics like the Jaccard similarity between sets, or counting distinct
-- items in the union/intersection of multiple data streams.
--
-- The core idea: hash each item to a uniform value in [0, 2^64). Keep only
-- hash values below a threshold theta. When the number of retained entries
-- exceeds k, lower theta to keep approximately k entries. The cardinality
-- estimate is (retained entries) / (theta / 2^64).
--
-- Set operations work by combining hash sets with appropriate theta adjustments.
--
-- Standard error is approximately 1/sqrt(k).
--
-- Items must be represented as Word64 values. Apply a hash function to convert
-- your domain types before inserting.
module DataSketches.Distinct.Theta
  ( -- * Construction
    ThetaSketch
  , mkThetaSketch
  -- * Updating
  , insert
  -- * Querying
  , estimate
  , isEmpty
  -- * Set operations
  , union
  , intersection
  , difference
  ) where

import Control.Monad.Primitive (PrimMonad(PrimState))
import Data.Word (Word64)
import DataSketches.Distinct.Theta.Internal

-- | Insert an item into the sketch. The item should be a hash of the original value.
insert :: PrimMonad m => ThetaSketch (PrimState m) -> Word64 -> m ()
insert = thetaInsert

-- | Estimate the number of distinct items inserted.
estimate :: PrimMonad m => ThetaSketch (PrimState m) -> m Double
estimate = thetaEstimate

-- | True if no items have been inserted.
isEmpty :: PrimMonad m => ThetaSketch (PrimState m) -> m Bool
isEmpty = thetaIsEmpty

-- | Compute the union of two sketches. Returns a new sketch.
union :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
union = thetaUnion

-- | Compute the intersection of two sketches. Returns a new sketch.
intersection :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
intersection = thetaIntersection

-- | Compute the set difference (A not B). Returns a new sketch containing
-- items that are in the first sketch but not the second.
difference :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
difference = thetaDifference
