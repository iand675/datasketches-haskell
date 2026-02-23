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

import Control.DeepSeq (NFData(..))
import Control.Monad (forM_)
import Control.Monad.Primitive
import Data.Bits (xor, shiftR)
import Data.Word
import Foreign.Ptr (Ptr, castPtr)
import Data.Primitive.ByteArray
import DataSketches.Core.Internal.URef
import DataSketches.Core.Internal.CBindings

data CountMinSketch s = CountMinSketch
  { cmsTable :: {-# UNPACK #-} !(MutableByteArray s)
  , cmsSeedArr :: {-# UNPACK #-} !(MutableByteArray s)
  , cmsCols :: {-# UNPACK #-} !Int
  , cmsRows :: {-# UNPACK #-} !Int
  , cmsTotalN :: {-# UNPACK #-} !(URef s Word64)
  }

instance NFData (CountMinSketch s) where rnf !_ = ()

murmurMix :: Word64 -> Word64
murmurMix h0 =
  let !h1 = (h0 `xor` (h0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
      !h2 = (h1 `xor` (h1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
  in h2 `xor` (h2 `shiftR` 33)
{-# INLINE murmurMix #-}

mkCountMinSketch :: PrimMonad m
  => Double -> Double -> m (CountMinSketch (PrimState m))
mkCountMinSketch epsilon delta = do
  let w = ceiling (exp 1 / epsilon) :: Int
      d = ceiling (log (1 / delta)) :: Int
  table <- newByteArray (w * d * 8)
  forM_ [0 .. w * d - 1] $ \i -> writeByteArray table i (0 :: Word64)
  seedArr <- newByteArray (d * 8)
  forM_ [0 .. d - 1] $ \i ->
    writeByteArray seedArr i (murmurMix (fromIntegral i * 0x9E3779B97F4A7C15 + 0x517CC1B727220A95))
  totalN <- newURef 0
  pure CountMinSketch
    { cmsTable = table
    , cmsSeedArr = seedArr
    , cmsCols = w
    , cmsRows = d
    , cmsTotalN = totalN
    }

cmsWidth :: CountMinSketch s -> Int
cmsWidth = cmsCols

cmsDepth :: CountMinSketch s -> Int
cmsDepth = cmsRows

-- | Insert via C FFI — hashes all rows and increments in one C call.
cmsInsert :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m ()
cmsInsert cms item = cmsInsertN cms item 1
{-# INLINE cmsInsert #-}

-- Pure Haskell insert: per-item FFI crossing is too expensive for the
-- small amount of work (d hash+increment ops, d typically 5-7).
cmsInsertN :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> Word64 -> m ()
cmsInsertN cms item n = do
  let !w = cmsCols cms
      !d = cmsRows cms
  go 0
  modifyURef (cmsTotalN cms) (+ n)
  where
    go !r
      | r >= cmsRows cms = pure ()
      | otherwise = do
          seed <- readByteArray (cmsSeedArr cms) r
          let !h = murmurMix ((seed :: Word64) `xor` item)
              !col = fromIntegral (h `mod` fromIntegral (cmsCols cms))
              !idx = r * cmsCols cms + col
          old <- readByteArray (cmsTable cms) idx
          writeByteArray (cmsTable cms) idx ((old :: Word64) + n)
          go (r + 1)
{-# INLINE cmsInsertN #-}

-- | Estimate via C FFI — hashes all rows and returns min in one call.
cmsEstimate :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m Word64
cmsEstimate cms item = unsafePrimToPrim $
  c_cms_estimate (castPtr $ mutableByteArrayContents (cmsTable cms))
                 (castPtr $ mutableByteArrayContents (cmsSeedArr cms))
                 (fromIntegral (cmsRows cms))
                 (fromIntegral (cmsCols cms))
                 item

cmsMerge :: PrimMonad m => CountMinSketch (PrimState m) -> CountMinSketch (PrimState m) -> m ()
cmsMerge this other = do
  let n = cmsRows this * cmsCols this
  forM_ [0..n-1] $ \i -> do
    otherVal <- readByteArray (cmsTable other) i
    thisVal <- readByteArray (cmsTable this) i
    writeByteArray (cmsTable this) i (thisVal + (otherVal :: Word64))
  otherN <- readURef (cmsTotalN other)
  modifyURef (cmsTotalN this) (+ otherN)
