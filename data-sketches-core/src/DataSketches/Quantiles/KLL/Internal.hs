{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Quantiles.KLL.Internal
  ( KllSketch
  , mkKllSketch
  , kllInsert
  , kllCount
  , kllMinimum
  , kllMaximum
  , kllQuantile
  , kllRank
  , kllMerge
  , kllRetainedItems
  , kllIsEmpty
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (unless)
import Control.Monad.Primitive
import Data.IORef
import Data.Word
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

-- | Opaque C struct, managed by a ForeignPtr with a destructor.
-- The entire sketch lives in C-allocated memory: no GC pressure,
-- no monadic bind overhead, no dictionary passing.
newtype KllSketch s = KllSketch (ForeignPtr ())

instance NFData (KllSketch s) where rnf !_ = ()

foreign import ccall unsafe "kll_new"      c_kll_new      :: Word32 -> Word64 -> IO (Ptr ())
foreign import ccall unsafe "&kll_free"    c_kll_free     :: FunPtr (Ptr () -> IO ())
foreign import ccall unsafe "kll_insert"   c_kll_insert   :: Ptr () -> CDouble -> IO ()
foreign import ccall unsafe "kll_count"    c_kll_count    :: Ptr () -> IO Word64
foreign import ccall unsafe "kll_min"      c_kll_min      :: Ptr () -> IO CDouble
foreign import ccall unsafe "kll_max"      c_kll_max      :: Ptr () -> IO CDouble
foreign import ccall unsafe "kll_is_empty" c_kll_is_empty :: Ptr () -> IO CInt
foreign import ccall unsafe "kll_retained" c_kll_retained :: Ptr () -> IO CInt
foreign import ccall unsafe "kll_rank"     c_kll_rank     :: Ptr () -> CDouble -> IO CDouble
foreign import ccall unsafe "kll_quantile" c_kll_quantile :: Ptr () -> CDouble -> IO CDouble
foreign import ccall unsafe "kll_merge"    c_kll_merge    :: Ptr () -> Ptr () -> IO ()

withSketch :: KllSketch s -> (Ptr () -> IO a) -> IO a
withSketch (KllSketch fp) = withForeignPtr fp
{-# INLINE withSketch #-}

mkKllSketch :: PrimMonad m => Word32 -> m (KllSketch (PrimState m))
mkKllSketch k = unsafePrimToPrim $ do
  unless (k >= 8) $ error "KLL sketch: k must be >= 8"
  ptr <- c_kll_new k 12345
  fp <- newForeignPtr c_kll_free ptr
  pure (KllSketch fp)

kllInsert :: PrimMonad m => KllSketch (PrimState m) -> Double -> m ()
kllInsert sk val = unsafePrimToPrim $ withSketch sk $ \p ->
  c_kll_insert p (realToFrac val)
{-# INLINE kllInsert #-}

kllCount :: PrimMonad m => KllSketch (PrimState m) -> m Word64
kllCount sk = unsafePrimToPrim $ withSketch sk c_kll_count
{-# INLINE kllCount #-}

kllMinimum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMinimum sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_kll_min
{-# INLINE kllMinimum #-}

kllMaximum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMaximum sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_kll_max
{-# INLINE kllMaximum #-}

kllIsEmpty :: PrimMonad m => KllSketch (PrimState m) -> m Bool
kllIsEmpty sk = unsafePrimToPrim $ withSketch sk $ fmap (/= 0) . c_kll_is_empty
{-# INLINE kllIsEmpty #-}

kllRetainedItems :: PrimMonad m => KllSketch (PrimState m) -> m Int
kllRetainedItems sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_kll_retained
{-# INLINE kllRetainedItems #-}

kllRank :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
kllRank sk value = unsafePrimToPrim $ withSketch sk $ \p ->
  realToFrac <$> c_kll_rank p (realToFrac value)
{-# INLINE kllRank #-}

kllQuantile :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
kllQuantile sk normRank = unsafePrimToPrim $ withSketch sk $ \p ->
  realToFrac <$> c_kll_quantile p (realToFrac normRank)
{-# INLINE kllQuantile #-}

kllMerge :: PrimMonad m => KllSketch (PrimState m) -> KllSketch (PrimState m) -> m ()
kllMerge dst src = unsafePrimToPrim $
  withSketch dst $ \pd ->
    withSketch src $ \ps ->
      c_kll_merge pd ps
