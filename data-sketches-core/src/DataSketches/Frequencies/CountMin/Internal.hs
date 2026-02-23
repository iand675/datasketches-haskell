{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Frequencies.CountMin.Internal
  ( CountMinSketch
  , mkCountMinSketch
  , cmsInsert
  , cmsInsertN
  , cmsEstimate
  , cmsMerge
  , cmsWidth
  , cmsDepth
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad.Primitive
import Data.Word
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

newtype CountMinSketch s = CountMinSketch (ForeignPtr ())
instance NFData (CountMinSketch s) where rnf !_ = ()

foreign import ccall unsafe "cms_new"        c_cms_new      :: CDouble -> CDouble -> IO (Ptr ())
foreign import ccall unsafe "&cms_free"      c_cms_free     :: FunPtr (Ptr () -> IO ())
foreign import ccall unsafe "cms_c_insert"   c_cms_insert   :: Ptr () -> Word64 -> Word64 -> IO ()
foreign import ccall unsafe "cms_c_estimate" c_cms_estimate :: Ptr () -> Word64 -> IO Word64
foreign import ccall unsafe "cms_c_merge"    c_cms_merge    :: Ptr () -> Ptr () -> IO ()
foreign import ccall unsafe "cms_c_width"    c_cms_width    :: Ptr () -> IO CInt
foreign import ccall unsafe "cms_c_depth"    c_cms_depth    :: Ptr () -> IO CInt

withSketch :: CountMinSketch s -> (Ptr () -> IO a) -> IO a
withSketch (CountMinSketch fp) = withForeignPtr fp
{-# INLINE withSketch #-}

mkCountMinSketch :: PrimMonad m => Double -> Double -> m (CountMinSketch (PrimState m))
mkCountMinSketch epsilon delta = unsafePrimToPrim $ do
  ptr <- c_cms_new (realToFrac epsilon) (realToFrac delta)
  fp <- newForeignPtr c_cms_free ptr
  pure (CountMinSketch fp)

cmsInsert :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m ()
cmsInsert sk item = cmsInsertN sk item 1
{-# INLINE cmsInsert #-}

cmsInsertN :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> Word64 -> m ()
cmsInsertN sk item n = unsafePrimToPrim $ withSketch sk $ \p -> c_cms_insert p item n
{-# INLINE cmsInsertN #-}

cmsEstimate :: PrimMonad m => CountMinSketch (PrimState m) -> Word64 -> m Word64
cmsEstimate sk item = unsafePrimToPrim $ withSketch sk $ \p -> c_cms_estimate p item

cmsMerge :: PrimMonad m => CountMinSketch (PrimState m) -> CountMinSketch (PrimState m) -> m ()
cmsMerge dst src = unsafePrimToPrim $
  withSketch dst $ \pd -> withSketch src $ \ps -> c_cms_merge pd ps

cmsWidth :: PrimMonad m => CountMinSketch (PrimState m) -> m Int
cmsWidth sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_cms_width

cmsDepth :: PrimMonad m => CountMinSketch (PrimState m) -> m Int
cmsDepth sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_cms_depth
