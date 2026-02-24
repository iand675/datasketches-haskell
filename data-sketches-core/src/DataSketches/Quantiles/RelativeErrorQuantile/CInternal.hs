{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Quantiles.RelativeErrorQuantile.CInternal
  ( CReqSketch
  , mkCReqSketch
  , creqInsert
  , creqInsertBatch
  , creqMerge
  , creqCount
  , creqIsEmpty
  , creqMin
  , creqMax
  , creqSum
  , creqRetained
  , creqK
  , creqRankAccuracy
  , creqNumLevels
  , creqCriterion
  , creqSetCriterion
  , creqCountWithCriterion
  , creqRank
  , creqQuantile
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (unless)
import Control.Monad.Primitive
import Data.Word
import qualified Data.Vector.Storable as VS
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

newtype CReqSketch s = CReqSketch (ForeignPtr ())

instance NFData (CReqSketch s) where rnf !_ = ()

foreign import ccall unsafe "req_new"             c_req_new             :: Word32 -> CInt -> Word64 -> IO (Ptr ())
foreign import ccall unsafe "&req_free"           c_req_free            :: FunPtr (Ptr () -> IO ())
foreign import ccall unsafe "req_insert"           c_req_insert          :: Ptr () -> CDouble -> IO ()
foreign import ccall unsafe "req_insert_batch"     c_req_insert_batch    :: Ptr () -> Ptr CDouble -> CInt -> IO ()
foreign import ccall unsafe "req_merge"           c_req_merge           :: Ptr () -> Ptr () -> IO ()
foreign import ccall unsafe "req_count"           c_req_count           :: Ptr () -> IO Word64
foreign import ccall unsafe "req_is_empty"        c_req_is_empty        :: Ptr () -> IO CInt
foreign import ccall unsafe "req_min"             c_req_min             :: Ptr () -> IO CDouble
foreign import ccall unsafe "req_max"             c_req_max             :: Ptr () -> IO CDouble
foreign import ccall unsafe "req_sum"             c_req_sum             :: Ptr () -> IO CDouble
foreign import ccall unsafe "req_retained"        c_req_retained        :: Ptr () -> IO CInt
foreign import ccall unsafe "req_k"               c_req_k               :: Ptr () -> IO Word32
foreign import ccall unsafe "req_rank_accuracy"   c_req_rank_accuracy   :: Ptr () -> IO CInt
foreign import ccall unsafe "req_num_levels"      c_req_num_levels      :: Ptr () -> IO CInt
foreign import ccall unsafe "req_criterion"       c_req_criterion       :: Ptr () -> IO CInt
foreign import ccall unsafe "req_set_criterion"   c_req_set_criterion   :: Ptr () -> CInt -> IO ()
foreign import ccall unsafe "req_count_with_criterion" c_req_cwc        :: Ptr () -> CDouble -> IO Word64
foreign import ccall unsafe "req_rank"            c_req_rank            :: Ptr () -> CDouble -> IO CDouble
foreign import ccall unsafe "req_quantile"        c_req_quantile        :: Ptr () -> CDouble -> IO CDouble

withSketch :: CReqSketch s -> (Ptr () -> IO a) -> IO a
withSketch (CReqSketch fp) = withForeignPtr fp
{-# INLINE withSketch #-}

mkCReqSketch :: PrimMonad m => Word32 -> Int -> m (CReqSketch (PrimState m))
mkCReqSketch k ra = unsafePrimToPrim $ do
  unless (even k && k >= 4 && k <= 1024) $
    error "k must be divisible by 2, and satisfy 4 <= k <= 1024"
  ptr <- c_req_new k (fromIntegral ra) 12345
  fp <- newForeignPtr c_req_free ptr
  pure (CReqSketch fp)

creqInsert :: PrimMonad m => CReqSketch (PrimState m) -> Double -> m ()
creqInsert sk val = unsafePrimToPrim $ withSketch sk $ \p -> c_req_insert p (realToFrac val)
{-# INLINE creqInsert #-}

creqInsertBatch :: PrimMonad m => CReqSketch (PrimState m) -> VS.Vector Double -> m ()
creqInsertBatch sk vals = unsafePrimToPrim $ withSketch sk $ \p ->
  VS.unsafeWith (VS.unsafeCast vals) $ \arr ->
    c_req_insert_batch p arr (fromIntegral $ VS.length vals)

creqMerge :: PrimMonad m => CReqSketch (PrimState m) -> CReqSketch (PrimState m) -> m ()
creqMerge dst src = unsafePrimToPrim $
  withSketch dst $ \pd -> withSketch src $ \ps -> c_req_merge pd ps

creqCount :: PrimMonad m => CReqSketch (PrimState m) -> m Word64
creqCount sk = unsafePrimToPrim $ withSketch sk c_req_count
{-# INLINE creqCount #-}

creqIsEmpty :: PrimMonad m => CReqSketch (PrimState m) -> m Bool
creqIsEmpty sk = unsafePrimToPrim $ withSketch sk $ fmap (/= 0) . c_req_is_empty
{-# INLINE creqIsEmpty #-}

creqMin :: PrimMonad m => CReqSketch (PrimState m) -> m Double
creqMin sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_req_min
{-# INLINE creqMin #-}

creqMax :: PrimMonad m => CReqSketch (PrimState m) -> m Double
creqMax sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_req_max
{-# INLINE creqMax #-}

creqSum :: PrimMonad m => CReqSketch (PrimState m) -> m Double
creqSum sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_req_sum
{-# INLINE creqSum #-}

creqRetained :: PrimMonad m => CReqSketch (PrimState m) -> m Int
creqRetained sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_req_retained
{-# INLINE creqRetained #-}

creqK :: PrimMonad m => CReqSketch (PrimState m) -> m Word32
creqK sk = unsafePrimToPrim $ withSketch sk c_req_k

creqRankAccuracy :: PrimMonad m => CReqSketch (PrimState m) -> m Int
creqRankAccuracy sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_req_rank_accuracy

creqNumLevels :: PrimMonad m => CReqSketch (PrimState m) -> m Int
creqNumLevels sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_req_num_levels

creqCriterion :: PrimMonad m => CReqSketch (PrimState m) -> m Int
creqCriterion sk = unsafePrimToPrim $ withSketch sk $ fmap fromIntegral . c_req_criterion

creqSetCriterion :: PrimMonad m => CReqSketch (PrimState m) -> Int -> m ()
creqSetCriterion sk c = unsafePrimToPrim $ withSketch sk $ \p -> c_req_set_criterion p (fromIntegral c)

creqCountWithCriterion :: PrimMonad m => CReqSketch (PrimState m) -> Double -> m Word64
creqCountWithCriterion sk val = unsafePrimToPrim $ withSketch sk $ \p -> c_req_cwc p (realToFrac val)

creqRank :: PrimMonad m => CReqSketch (PrimState m) -> Double -> m Double
creqRank sk val = unsafePrimToPrim $ withSketch sk $ \p -> realToFrac <$> c_req_rank p (realToFrac val)
{-# INLINE creqRank #-}

creqQuantile :: PrimMonad m => CReqSketch (PrimState m) -> Double -> m Double
creqQuantile sk nr = unsafePrimToPrim $ withSketch sk $ \p -> realToFrac <$> c_req_quantile p (realToFrac nr)
{-# INLINE creqQuantile #-}
