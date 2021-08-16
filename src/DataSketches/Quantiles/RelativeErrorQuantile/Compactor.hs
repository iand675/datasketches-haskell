{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
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


data CompactorReturn = CompactorReturn
  { deltaRetItems :: !Int
  , deltaNominalSize :: !Int
  }

data ReqCompactor s (lgWeight :: Nat) = ReqCompactor 
  -- Configuration constants
  { rcRankAccuracy :: !RankAccuracy
  -- State
  , rcState :: !Word64
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

getBuffer :: ReqCompactor s k -> DoubleBuffer s
getBuffer = rcBuffer

getCoin :: (PrimMonad m, MonadIO m) => m Bool
getCoin = create >>= uniform

getLgWeight :: KnownNat n => ReqCompactor s n -> Word8
getLgWeight = fromIntegral . natVal

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
getState = pure . rcState

isHighRankAccuracy :: PrimMonad m => ReqCompactor (PrimState m) k -> m Bool
isHighRankAccuracy = pure . (HighRanksAreAccurate ==) . rcRankAccuracy

merge 
  :: (PrimMonad m, s ~ PrimState m)
  => ReqCompactor (PrimState m) lgWeight 
  -> ReqCompactor (PrimState m) lgWeight 
  -> m (ReqCompactor s lgWeight)
merge compactorA compactorB = do
  _ <- ensureEnoughSections compactorA
  let buff = rcBuffer compactorA
  _ <- sort buff
  otherBuff <- undefined -- copy the buffer from compactorB
  _ <- sort otherBuff
  otherBuffIsBigger <- (>) <$> getCount otherBuff <*> getCount buff
  finalBuff <- if otherBuffIsBigger
     then mergeSortIn otherBuff buff
     else mergeSortIn buff otherBuff
  pure $ compactorA
    { rcState = rcState compactorA .|. rcState compactorB
    , rcBuffer = finalBuff
    }

ensureEnoughSections :: PrimMonad m => ReqCompactor (PrimState m) a -> m Bool
ensureEnoughSections compactor = do
  let szf = rcSectionSizeFlt compactor / sqrt2
      ne = nearestEven szf
  if (rcState compactor >= (1 `shiftL` (fromInteger $ toInteger $ pred $ rcNumSections compactor)))
     && rcSectionSize compactor > minK
     && ne >= minK
     then undefined 
     else pure False

nearestEven :: Double -> Int
nearestEven = (shiftL 1) . round . (/ 2)
