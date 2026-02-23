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
import Control.Monad (when, forM_)
import Control.Monad.Primitive
import Data.Bits (shiftL, shiftR, (.&.), xor, countTrailingZeros)
import Data.Word
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek)
import Data.Primitive.ByteArray (MutableByteArray, mutableByteArrayContents, newByteArray, readByteArray, writeByteArray)
import DataSketches.Core.Internal.CBindings

data HllSketch s = HllSketch
  { hllRegisters :: {-# UNPACK #-} !(MutableByteArray s)
  , hllPrecisionBits :: {-# UNPACK #-} !Int
  , hllNumRegisters :: {-# UNPACK #-} !Int
  }

instance NFData (HllSketch s) where rnf !_ = ()

mkHllSketch :: PrimMonad m => Int -> m (HllSketch (PrimState m))
mkHllSketch p = do
  when (p < 4 || p > 26) $ error "HLL: precision must be in [4, 26]"
  let m = 1 `shiftL` p
  regs <- newByteArray m
  forM_ [0..m-1] $ \i -> writeByteArray regs i (0 :: Word8)
  pure HllSketch
    { hllRegisters = regs
    , hllPrecisionBits = p
    , hllNumRegisters = m
    }

hllPrecision :: HllSketch s -> Int
hllPrecision = hllPrecisionBits

-- | Pure Haskell insert on raw MutableByteArray — avoids FFI overhead
-- per item (the hash + single-byte update is too small for FFI to help).
hllInsert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
hllInsert sk !item =
  let !hash = murmurMix64 item
      !p = hllPrecisionBits sk
      !registerIdx = fromIntegral (hash .&. (fromIntegral (hllNumRegisters sk) - 1))
      !w = hash `shiftR` p
      !rho = countLeadingZerosW w (64 - p) + 1
      !rhoW8 = fromIntegral rho :: Word8
  in do
    currentVal <- readByteArray (hllRegisters sk) registerIdx
    when ((rhoW8 :: Word8) > currentVal) $
      writeByteArray (hllRegisters sk) registerIdx rhoW8
{-# INLINE hllInsert #-}

murmurMix64 :: Word64 -> Word64
murmurMix64 h0 =
  let !h1 = (h0 `xor` (h0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
      !h2 = (h1 `xor` (h1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
  in h2 `xor` (h2 `shiftR` 33)
{-# INLINE murmurMix64 #-}

countLeadingZerosW :: Word64 -> Int -> Int
countLeadingZerosW 0 bits = bits
countLeadingZerosW w bits =
  let !clz = countTrailingZeros w
  in min clz bits
{-# INLINE countLeadingZerosW #-}

-- | Estimate via C FFI. The C function computes the harmonic sum using
-- ldexp(1.0, -val) — a single FPU instruction per register, vs the
-- Haskell (^^) which went through Integer arithmetic.
hllEstimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
hllEstimate sk = unsafePrimToPrim $ do
  let m = hllNumRegisters sk
      mf = fromIntegral m :: Double
      ptr = mutableByteArrayContents (hllRegisters sk)
  (rawEstimate, _, zeroCount) <- alloca $ \pRaw -> alloca $ \pHarm -> alloca $ \pZero -> do
    c_hll_estimate ptr (fromIntegral m) pRaw pHarm pZero
    raw <- peek pRaw
    harm <- peek pHarm
    zc <- peek pZero
    pure (realToFrac raw :: Double, realToFrac harm :: Double, fromIntegral zc :: Int)

  let twoTo32 = 4294967296.0 :: Double
  if rawEstimate <= 2.5 * mf && zeroCount > 0
    then pure $ mf * log (mf / fromIntegral zeroCount)
    else
      if rawEstimate > twoTo32 / 30.0
        then pure $ negate twoTo32 * log (1.0 - rawEstimate / twoTo32)
        else pure rawEstimate

-- | Merge: take element-wise max of register arrays.
hllMerge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
hllMerge this other = do
  when (hllPrecisionBits this /= hllPrecisionBits other) $
    error "HLL: cannot merge sketches with different precision"
  let m = hllNumRegisters this
  forM_ [0..m-1] $ \i -> do
    thisVal <- readByteArray (hllRegisters this) i
    otherVal <- readByteArray (hllRegisters other) i
    when ((otherVal :: Word8) > thisVal) $
      writeByteArray (hllRegisters this) i otherVal
