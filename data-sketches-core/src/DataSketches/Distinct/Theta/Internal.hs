{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Distinct.Theta.Internal
  ( ThetaSketch
  , mkThetaSketch
  , thetaInsert
  , thetaEstimate
  , thetaUnion
  , thetaIntersection
  , thetaDifference
  , thetaIsEmpty
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (when)
import Control.Monad.Primitive
import Data.Word
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Ptr

newtype ThetaSketch s = ThetaSketch (ForeignPtr ())
instance NFData (ThetaSketch s) where rnf !_ = ()

foreign import ccall unsafe "theta_new"              c_theta_new          :: CInt -> IO (Ptr ())
foreign import ccall unsafe "&theta_free"            c_theta_free         :: FunPtr (Ptr () -> IO ())
foreign import ccall unsafe "theta_c_insert"         c_theta_insert       :: Ptr () -> Word64 -> IO ()
foreign import ccall unsafe "theta_c_estimate"       c_theta_estimate     :: Ptr () -> IO CDouble
foreign import ccall unsafe "theta_c_is_empty"       c_theta_is_empty     :: Ptr () -> IO CInt
foreign import ccall unsafe "theta_c_union"          c_theta_union        :: Ptr () -> Ptr () -> IO (Ptr ())
foreign import ccall unsafe "theta_c_intersection"   c_theta_intersection :: Ptr () -> Ptr () -> IO (Ptr ())
foreign import ccall unsafe "theta_c_difference"     c_theta_difference   :: Ptr () -> Ptr () -> IO (Ptr ())

withSketch :: ThetaSketch s -> (Ptr () -> IO a) -> IO a
withSketch (ThetaSketch fp) = withForeignPtr fp
{-# INLINE withSketch #-}

wrapNew :: Ptr () -> IO (ThetaSketch s)
wrapNew ptr = ThetaSketch <$> newForeignPtr c_theta_free ptr

mkThetaSketch :: PrimMonad m => Int -> m (ThetaSketch (PrimState m))
mkThetaSketch k = unsafePrimToPrim $ do
  when (k < 16) $ error "Theta: k must be >= 16"
  ptr <- c_theta_new (fromIntegral k)
  wrapNew ptr

thetaInsert :: PrimMonad m => ThetaSketch (PrimState m) -> Word64 -> m ()
thetaInsert sk !item = unsafePrimToPrim $ withSketch sk $ \p -> c_theta_insert p item
{-# INLINE thetaInsert #-}

thetaEstimate :: PrimMonad m => ThetaSketch (PrimState m) -> m Double
thetaEstimate sk = unsafePrimToPrim $ withSketch sk $ fmap realToFrac . c_theta_estimate

thetaIsEmpty :: PrimMonad m => ThetaSketch (PrimState m) -> m Bool
thetaIsEmpty sk = unsafePrimToPrim $ withSketch sk $ fmap (/= 0) . c_theta_is_empty

thetaUnion :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaUnion a b = unsafePrimToPrim $
  withSketch a $ \pa -> withSketch b $ \pb -> do
    ptr <- c_theta_union pa pb
    wrapNew ptr

thetaIntersection :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaIntersection a b = unsafePrimToPrim $
  withSketch a $ \pa -> withSketch b $ \pb -> do
    ptr <- c_theta_intersection pa pb
    wrapNew ptr

thetaDifference :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaDifference a b = unsafePrimToPrim $
  withSketch a $ \pa -> withSketch b $ \pb -> do
    ptr <- c_theta_difference pa pb
    wrapNew ptr
