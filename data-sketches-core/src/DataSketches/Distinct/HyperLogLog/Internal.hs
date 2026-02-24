{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Distinct.HyperLogLog.Internal
  ( HllSketch
  , mkHllSketch
  , hllInsert
  , hllInsertBatch
  , hllEstimate
  , hllMerge
  , hllPrecision
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (when)
import Control.Monad.Primitive
import Data.Word
import qualified Data.Vector.Storable as VS
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

newtype HllSketch s = HllSketch (ForeignPtr (HllSketch s))
instance NFData (HllSketch s) where rnf !_ = ()

foreign import ccall unsafe "hll_new"        c_hll_new      :: CInt -> IO (Ptr (HllSketch s))
foreign import ccall unsafe "&hll_free"      c_hll_free     :: FunPtr (Ptr (HllSketch s) -> IO ())
foreign import ccall unsafe "hll_c_insert"       c_hll_insert       :: Ptr (HllSketch s) -> Word64 -> IO ()
foreign import ccall unsafe "hll_c_insert_batch" c_hll_insert_batch :: Ptr (HllSketch s) -> Ptr Word64 -> CInt -> IO ()
foreign import ccall unsafe "hll_c_estimate"     c_hll_estimate     :: Ptr (HllSketch s) -> IO CDouble
foreign import ccall unsafe "hll_c_merge"    c_hll_merge    :: Ptr (HllSketch s) -> Ptr (HllSketch s) -> IO ()
foreign import ccall unsafe "hll_c_precision" c_hll_precision :: Ptr (HllSketch s) -> IO CInt

mkHllSketch :: PrimMonad m => Int -> m (HllSketch (PrimState m))
mkHllSketch p = unsafePrimToPrim $ do
  when (p < 4 || p > 26) $ error "HLL: precision must be in [4, 26]"
  ptr <- c_hll_new (fromIntegral p)
  fp <- newForeignPtr c_hll_free ptr
  pure (HllSketch fp)

hllPrecision :: PrimMonad m => HllSketch (PrimState m) -> m Int
hllPrecision (HllSketch sk) = unsafePrimToPrim $ withForeignPtr sk $ fmap fromIntegral . c_hll_precision

hllInsert :: PrimMonad m => HllSketch (PrimState m) -> Word64 -> m ()
hllInsert (HllSketch sk) !item = unsafePrimToPrim $ withForeignPtr sk $ \p -> c_hll_insert p item
{-# INLINE hllInsert #-}

hllInsertBatch :: PrimMonad m => HllSketch (PrimState m) -> VS.Vector Word64 -> m ()
hllInsertBatch (HllSketch sk) vals = unsafePrimToPrim $ withForeignPtr sk $ \p ->
  VS.unsafeWith vals $ \arr ->
    c_hll_insert_batch p arr (fromIntegral $ VS.length vals)

hllEstimate :: PrimMonad m => HllSketch (PrimState m) -> m Double
hllEstimate (HllSketch sk) = unsafePrimToPrim $ withForeignPtr sk $ fmap realToFrac . c_hll_estimate

hllMerge :: PrimMonad m => HllSketch (PrimState m) -> HllSketch (PrimState m) -> m ()
hllMerge (HllSketch this) (HllSketch other) = unsafePrimToPrim $
  withForeignPtr this $ \pd -> withForeignPtr other $ \ps -> c_hll_merge pd ps

