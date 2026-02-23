{-# LANGUAGE MagicHash #-}
module DataSketches.Frequencies.CountMin.Internal
  ( CountMinSketch(..)
  , mkCountMinSketch
  , cmsInsert
  , cmsInsertN
  , cmsEstimate
  , cmsMerge
  , cmsWidth
  , cmsDepth
  ) where

import Control.Monad (forM_)
import Control.Monad.Primitive
import Data.Bits (xor, shiftR, shiftL, (.&.))
import Data.Primitive.MutVar
import Data.Word
import qualified Data.Vector.Unboxed.Mutable as MUVector
import DataSketches.Core.Internal.URef

-- | A Count-Min Sketch for frequency estimation.
-- Uses multiple hash functions (rows) and a fixed width (columns).
-- Each insertion hashes to one cell per row and increments it.
-- Queries return the minimum across all rows for the hashed positions.
data CountMinSketch s = CountMinSketch
  { cmsTable :: {-# UNPACK #-} !(MUVector.MVector s Word64)
  , cmsCols :: {-# UNPACK #-} !Int
  , cmsRows :: {-# UNPACK #-} !Int
  , cmsTotalN :: {-# UNPACK #-} !(URef s Word64)
  , cmsSeeds :: ![Word64]
  }

-- | Create a new Count-Min Sketch.
-- epsilon: error tolerance (e.g. 0.001 for 0.1% error)
-- delta: failure probability (e.g. 0.01 for 99% confidence)
mkCountMinSketch :: PrimMonad m
  => Double -- ^ epsilon (error tolerance, e.g. 0.001)
  -> Double -- ^ delta (failure probability, e.g. 0.01)
  -> m (CountMinSketch (PrimState m))
mkCountMinSketch epsilon delta = do
  let w = ceiling (exp 1 / epsilon) :: Int
      d = ceiling (log (1 / delta)) :: Int
  table <- MUVector.replicate (w * d) 0
  totalN <- newURef 0
  let seeds = generateSeeds d
  pure CountMinSketch
    { cmsTable = table
    , cmsCols = w
    , cmsRows = d
    , cmsTotalN = totalN
    , cmsSeeds = seeds
    }

generateSeeds :: Int -> [Word64]
generateSeeds d =
  let go !i acc
        | i >= d = acc
        | otherwise = go (i + 1) (murmurMix (fromIntegral i * 0x9E3779B97F4A7C15 + 0x517CC1B727220A95) : acc)
  in reverse (go 0 [])

-- Murmur3 finalizer for mixing
murmurMix :: Word64 -> Word64
murmurMix h0 =
  let h1 = (h0 `xor` (h0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
      h2 = (h1 `xor` (h1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
  in h2 `xor` (h2 `shiftR` 33)

hashItem :: Word64 -> Word64 -> Int -> Int
hashItem seed item width =
  let h = murmurMix (seed `xor` item)
  in fromIntegral (h `mod` fromIntegral width)

cmsWidth :: CountMinSketch s -> Int
cmsWidth = cmsCols

cmsDepth :: CountMinSketch s -> Int
cmsDepth = cmsRows

-- | Insert an item (represented as a Word64 hash) into the sketch.
cmsInsert :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m ()
cmsInsert = flip cmsInsertN 1

-- | Insert an item with a given count.
cmsInsertN :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> Word64 -> m ()
cmsInsertN cms item n = do
  let w = cmsCols cms
      seeds = cmsSeeds cms
  go seeds 0
  modifyURef (cmsTotalN cms) (+ n)
  where
    go [] _ = pure ()
    go (seed:rest) !row = do
      let col = hashItem seed item (cmsCols cms)
          idx = row * cmsCols cms + col
      MUVector.unsafeModify (cmsTable cms) (+ n) idx
      go rest (row + 1)

-- | Estimate the count of an item.
-- Returns the minimum count across all hash rows.
-- This is an upper bound on the true count; it may overcount but never undercount.
cmsEstimate :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m Word64
cmsEstimate cms item = do
  let seeds = cmsSeeds cms
  go seeds 0 maxBound
  where
    go [] _ !minVal = pure minVal
    go (seed:rest) !row !minVal = do
      let col = hashItem seed item (cmsCols cms)
          idx = row * cmsCols cms + col
      val <- MUVector.unsafeRead (cmsTable cms) idx
      go rest (row + 1) (min minVal val)

-- | Merge the second sketch into the first. Both must have same dimensions.
cmsMerge :: PrimMonad m => CountMinSketch (PrimState m) -> CountMinSketch (PrimState m) -> m ()
cmsMerge this other = do
  let n = MUVector.length (cmsTable this)
  forM_ [0..n-1] $ \i -> do
    otherVal <- MUVector.unsafeRead (cmsTable other) i
    MUVector.unsafeModify (cmsTable this) (+ otherVal) i
  otherN <- readURef (cmsTotalN other)
  modifyURef (cmsTotalN this) (+ otherN)
