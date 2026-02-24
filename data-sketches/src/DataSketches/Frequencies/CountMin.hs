-- | Count-Min sketch for approximate frequency estimation.
--
-- "How many times has /this particular item/ appeared?" — imagine counting
-- ad impressions per user, or requests per API key, when there are too many
-- distinct items to keep an exact counter for each one. Count-Min gives you
-- an approximate count using fixed memory.
--
-- It is conservative: estimates may overcount (due to hash collisions)
-- but /never/ undercount. If an item truly appeared 100 times, the
-- sketch will report >= 100. This makes it safe for rate limiting and
-- frequency capping — you might act slightly early, but never too late.
--
-- === Parameters
--
-- * /epsilon/ (ε): error tolerance. The estimate is within @ε * N@ of the
--   true count with high probability, where @N@ is the total insertions.
-- * /delta/ (δ): failure probability. The probability the estimate exceeds
--   the error bound is at most δ.
--
-- Space used is @O(1\/ε * log(1\/δ))@.
--
-- === Hashing
--
-- Items must be pre-hashed to 'Word64'. Apply a hash function to your
-- domain types before calling 'insert'.
--
-- === Implementation
--
-- Backed by a C implementation (@cbits\/countmin.c@) behind a 'ForeignPtr'.
-- All sketch memory lives outside the GHC heap.
--
-- === Usage
--
-- @
-- import qualified DataSketches.Frequencies.CountMin as CM
-- import Data.Hashable (hash)
--
-- main :: IO ()
-- main = do
--   sk <- CM.'mkCountMinSketch' 0.001 0.01  -- ε=0.1%, δ=1%
--   mapM_ (\\_ -> CM.'insert' sk 42) [1..500]
--   freq <- CM.'estimate' sk 42
--   putStrLn $ "frequency ≈ " ++ show freq  -- ≈ 500
-- @
--
-- === Mergeability
--
-- Fully mergeable via 'merge'. Both sketches must have been constructed
-- with the same ε and δ (i.e. identical dimensions).
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

import Control.Monad.Primitive (PrimMonad, PrimState)
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
width :: PrimMonad m => CountMinSketch (PrimState m) -> m Int
width = cmsWidth

-- | Number of rows (hash functions) in the sketch.
depth :: PrimMonad m => CountMinSketch (PrimState m) -> m Int
depth = cmsDepth
