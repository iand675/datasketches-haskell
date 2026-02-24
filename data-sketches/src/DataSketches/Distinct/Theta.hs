-- | Theta sketch for distinct counting with full set algebra.
--
-- Like HyperLogLog, Theta counts distinct items — but it can also do set
-- math. "How many users visited /both/ page A and page B?" (intersection).
-- "How many visited A but /not/ B?" (difference). "How many visited
-- /either/?" (union). You can even chain these into complex expressions
-- like @(A ∪ B) ∩ (C ∪ D) \\ E@, and the result is itself a sketch
-- that you can query or combine further.
--
-- === Algorithm
--
-- Each item is hashed to a uniform value in @[0, 2^64)@. The sketch retains
-- only hashes below a threshold /theta/. When the number of retained entries
-- exceeds @k@, theta is lowered to keep approximately @k@ entries. The
-- cardinality estimate is @(retained entries) / (theta / 2^64)@.
--
-- Set operations combine hash sets with appropriate theta adjustments — the
-- result is itself a sketch that can participate in further set operations.
--
-- Standard error is approximately @1 / sqrt(k)@.
--
-- === Hashing
--
-- Items must be pre-hashed to 'Word64'. Apply a good hash function to your
-- domain types before calling 'insert'.
--
-- === Implementation
--
-- Backed by a C implementation (@cbits\/theta.c@) behind a 'ForeignPtr'.
-- All sketch memory lives outside the GHC heap.
--
-- === Usage
--
-- @
-- import qualified DataSketches.Distinct.Theta as Theta
--
-- main :: IO ()
-- main = do
--   a <- Theta.'mkThetaSketch' 4096
--   b <- Theta.'mkThetaSketch' 4096
--   mapM_ (Theta.'insert' a) [1..1000]
--   mapM_ (Theta.'insert' b) [500..1500]
--
--   both <- Theta.'intersection' a b
--   overlap <- Theta.'estimate' both
--   putStrLn $ "overlap ≈ " ++ show overlap  -- ≈ 501
--
--   onlyA <- Theta.'difference' a b
--   exclusive <- Theta.'estimate' onlyA
--   putStrLn $ "only in A ≈ " ++ show exclusive  -- ≈ 499
-- @
--
-- === When to use HyperLogLog instead
--
-- If you only need cardinality estimation (no set operations),
-- "DataSketches.Distinct.HyperLogLog" uses less memory for the same accuracy.
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
