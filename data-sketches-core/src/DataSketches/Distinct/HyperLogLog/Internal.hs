{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Distinct.HyperLogLog.Internal
  ( HllSketch
  , mkHllSketch
  , hllInsert
  , hllEstimate
  , hllMerge
  , hllPrecision
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (when)
import Control.Monad.Primitive
import Data.Word
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

newtype HllSketch s = HllSketch (ForeignPtr ())
instance NFData (HllSketch s) where rnf !_ = ()

foreign import ccall unsafe "hll_new"        c_hll_new      :: CInt -> IO (Ptr ())
foreign import ccall unsafe "&hll_free"      c_hll_free     :: FunPtr (Ptr () -> IO ())
foreign import ccall unsafe "hll_c_insert"   c_hll_insert   :: Ptr () -> Word64 -> IO ()
foreign import ccall unsafe "hll_c_estimate" c_hll_estimate :: Ptr () -> IO CDouble
foreign import ccall unsafe "hll_c_merge"    c_hll_merge    :: Ptr () -> Ptr () -> IO ()
foreign import ccall unsafe "hll_c_precision" c_hll_precision :: Ptr () -> IO CInt

withSketch :: HllSketch s -> (Ptr () -> IO a) -> IO a
withSketch (HllSketch fp) = withForeignPtr fp
{-# INLINE withSketch #-}

mkHllSketch :: PrimMonad m => Int -> m (HllSketch (PrimState m))
mkHllSketch p = unsafePrimToPrim $ do
  when (p < 4 || p > 26) $ error "HLL: precision must be in [4, 26]"
  ptr <- c_hll_new (fromIntegral p)
  fp <- newForeignPtr c_hll_free ptr
  pure (HllSketch fp)

hllPrecision :: PrimMonad m => HllSketch (PrimState m) -> m Int
hllPrecision sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_hll_precision

hllInsert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
hllInsert sk !item = unsafePrimToPrim $ withSketch sk $ \p -> c_hll_insert p item
{-# INLINE hllInsert #-}

hllEstimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
hllEstimate sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_hll_estimate

hllMerge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
hllMerge this other = unsafePrimToPrim $
  withSketch this $ \pd -> withSketch other $ \ps -> c_hll_merge pd ps
