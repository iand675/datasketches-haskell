{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
module DataSketches.Quantiles.RelativeErrorQuantile.Compactor
  ( ReqCompactor
  , CompactorReturn
  , compact
  , getBuffer
  , getCoin
  , getLgWeight
  , getNominalCapacity
  , getNumSections
  , merge
  ) where

import GHC.TypeLits
import Data.Bits ((.|.), shiftL)
import Data.Semigroup (Semigroup)
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import System.Random.MWC (create, Variate(uniform))
import Control.Monad.Trans
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer
import DataSketches.Quantiles.RelativeErrorQuantile.URef


data CompactorReturn = CompactorReturn
  { deltaRetItems :: !Int
  , deltaNominalSize :: !Int
  }

data ReqCompactor s (lgWeight :: Nat) = ReqCompactor 
  -- Configuration constants
  { rcRankAccuracy :: !RankAccuracy
  -- State
  , rcState :: !(URef s Word64)
  , rcSectionSizeFlt :: !Double
  , rcSectionSize :: !Word32
  , rcNumSections :: !Word8
  , rcBuffer :: DoubleBuffer s
  }

sqrt2 :: Double
sqrt2 = sqrt 2

minK :: Num a => a
minK = 4

nomCapMult :: Num a => a
nomCapMult = 2

compact :: PrimMonad m => ReqCompactor (PrimState m) k -> CompactorReturn -> m (DoubleBuffer (PrimState m))
compact = undefined

getBuffer :: PrimMonad m => ReqCompactor (PrimState m) k -> m (DoubleBuffer (PrimState m))
getBuffer = pure . rcBuffer

getCoin :: (PrimMonad m, MonadIO m) => m Bool
getCoin = create >>= uniform

getLgWeight :: PrimMonad m => m Word8
getLgWeight = undefined

getNominalCapacity :: PrimMonad m => ReqCompactor (PrimState m) k -> m Int
getNominalCapacity compactor = pure $ nomCapMult * (toInt $ rcNumSections compactor) * (toInt $ rcSectionSize compactor)
  where 
    toInt :: Integral a => a -> Int
    toInt = fromInteger . toInteger

getNumSections :: PrimMonad m => ReqCompactor (PrimState m) k -> m Word8
getNumSections = pure . rcNumSections

getSectionSizeFlt :: PrimMonad m => ReqCompactor (PrimState m) k -> m Double
getSectionSizeFlt = pure . rcSectionSizeFlt

getState :: PrimMonad m => ReqCompactor (PrimState m) k -> m Word64
getState = readURef . rcState

isHighRankAccuracy :: PrimMonad m => ReqCompactor (PrimState m) k -> m Bool
isHighRankAccuracy = pure . (HighRanksAreAccurate ==) . rcRankAccuracy

merge 
  :: forall m lgWeight. PrimMonad m 
  => ReqCompactor (PrimState m) lgWeight 
  -> ReqCompactor (PrimState m) lgWeight 
  -> m (ReqCompactor (PrimState m) lgWeight)
merge compactorA compactorB = do
  _ <- ensureEnoughSections compactorA
  let buff = rcBuffer compactorA
  _ <- sort buff
  otherBuff <- undefined -- copy the buffer from compactorB
  _ <- sort otherBuff
  finalBuff <- if (getCount otherBuff) > (getCount buff)
     then mergeSortIn otherBuff buff
     else mergeSortIn buff otherBuff
  pure (compactorA
    { rcState = rcState compactorA .|. rcState compactorB
    , rcBuffer = finalBuff
    } :: ReqCompactor (PrimState m) k)

ensureEnoughSections :: PrimMonad m => ReqCompactor (PrimState m) a -> m Bool
ensureEnoughSections compactor = do
  let szf = rcSectionSizeFlt compactor / sqrt2
      ne = nearestEven szf
  state <- readURef $ rcState compactor
  if (state >= (1 `shiftL` (fromInteger $ toInteger $ pred $ rcNumSections compactor)))
     && rcSectionSize compactor > minK
     && ne >= minK
     then undefined 
     else pure False

nearestEven :: Double -> Int
nearestEven = (shiftL 1) . round . (/ 2)
