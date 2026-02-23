{-# LANGUAGE MagicHash #-}
module DataSketches.Distinct.HyperLogLog.Internal
  ( HllSketch(..)
  , mkHllSketch
  , hllInsert
  , hllEstimate
  , hllMerge
  , hllPrecision
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (forM_, when)
import Control.Monad.Primitive
import Data.Bits
import Data.Word
import qualified Data.Vector.Unboxed.Mutable as MUVector
import DataSketches.Core.Internal.URef

-- | HyperLogLog sketch for cardinality (distinct count) estimation.
--
-- Uses p bits of hash to index into 2^p registers. Each register stores
-- the maximum number of leading zeros + 1 seen in the remaining hash bits.
-- The harmonic mean of 2^(-register) values gives the cardinality estimate.
data HllSketch s = HllSketch
  { hllRegisters :: {-# UNPACK #-} !(MUVector.MVector s Word8)
  , hllPrecisionBits :: {-# UNPACK #-} !Int
  , hllNumRegisters :: {-# UNPACK #-} !Int
  }

instance NFData (HllSketch s) where rnf !_ = ()

murmurMix64 :: Word64 -> Word64
murmurMix64 h0 =
  let !h1 = (h0 `xor` (h0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
      !h2 = (h1 `xor` (h1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
  in h2 `xor` (h2 `shiftR` 33)
{-# INLINE murmurMix64 #-}

-- | Create a new HyperLogLog sketch with the given precision.
-- precision p means 2^p registers are used.
-- p must be in [4, 26]. Higher p gives better accuracy but uses more space.
-- Standard error is approximately 1.04 / sqrt(2^p).
-- p=12 gives ~1.6% error with 4KB of memory.
mkHllSketch :: PrimMonad m => Int -> m (HllSketch (PrimState m))
mkHllSketch p = do
  when (p < 4 || p > 26) $ error "HLL: precision must be in [4, 26]"
  let m = 1 `shiftL` p
  regs <- MUVector.replicate m 0
  pure HllSketch
    { hllRegisters = regs
    , hllPrecisionBits = p
    , hllNumRegisters = m
    }

hllPrecision :: HllSketch s -> Int
hllPrecision = hllPrecisionBits

-- | Insert an item (as a Word64 hash) into the sketch.
hllInsert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
hllInsert sk !item =
  let !hash = murmurMix64 item
      !p = hllPrecisionBits sk
      !registerIdx = fromIntegral (hash .&. (fromIntegral (hllNumRegisters sk) - 1))
      !w = hash `shiftR` p
      !rho = countLeadingZerosW w (64 - p) + 1
      !rhoW8 = fromIntegral rho :: Word8
  in do
    currentVal <- MUVector.unsafeRead (hllRegisters sk) registerIdx
    when (rhoW8 > currentVal) $
      MUVector.unsafeWrite (hllRegisters sk) registerIdx rhoW8
{-# INLINE hllInsert #-}

countLeadingZerosW :: Word64 -> Int -> Int
countLeadingZerosW 0 bits = bits
countLeadingZerosW w bits =
  let !clz = countTrailingZeros w
  in min clz bits
{-# INLINE countLeadingZerosW #-}

-- | Estimate the cardinality (number of distinct items).
hllEstimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
hllEstimate sk = do
  let m = hllNumRegisters sk
      mf = fromIntegral m :: Double
  -- Compute the harmonic mean indicator
  (harmonicSum, zeroCount) <- computeIndicator sk
  let alpha = alphaM m
      rawEstimate = alpha * mf * mf / harmonicSum
  -- Apply corrections
  if rawEstimate <= 2.5 * mf && zeroCount > 0
    then pure $ mf * log (mf / fromIntegral zeroCount) -- linear counting for small cardinalities
    else
      if rawEstimate > twoTo32 / 30.0
        then pure $ negate twoTo32 * log (1.0 - rawEstimate / twoTo32) -- large range correction
        else pure rawEstimate
  where
    twoTo32 = 4294967296.0 :: Double

computeIndicator :: PrimMonad m => HllSketch (PrimState m) -> m (Double, Int)
computeIndicator sk = do
  let m = hllNumRegisters sk
  go 0 0.0 0
  where
    go !i !acc !zeros
      | i >= hllNumRegisters sk = pure (acc, zeros)
      | otherwise = do
          val <- MUVector.unsafeRead (hllRegisters sk) i
          let z = if val == 0 then 1 else 0
          go (i + 1) (acc + 1.0 / (2.0 ^^ fromIntegral val)) (zeros + z)

-- Bias correction constant alpha_m
alphaM :: Int -> Double
alphaM m
  | m == 16 = 0.673
  | m == 32 = 0.697
  | m == 64 = 0.709
  | otherwise = 0.7213 / (1.0 + 1.079 / fromIntegral m)

-- | Merge the second sketch into the first. Both must have the same precision.
hllMerge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
hllMerge this other = do
  when (hllPrecisionBits this /= hllPrecisionBits other) $
    error "HLL: cannot merge sketches with different precision"
  let m = hllNumRegisters this
  forM_ [0..m-1] $ \i -> do
    thisVal <- MUVector.unsafeRead (hllRegisters this) i
    otherVal <- MUVector.unsafeRead (hllRegisters other) i
    when (otherVal > thisVal) $
      MUVector.unsafeWrite (hllRegisters this) i otherVal
