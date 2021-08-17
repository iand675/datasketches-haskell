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
import Data.Bits ((.&.), (.|.), complement, countTrailingZeros, shiftL, shiftR)
import Data.Primitive.MutVar
import Data.Semigroup (Semigroup)
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import System.Random.MWC (create, Variate(uniform))
import Control.Monad (when)
import Control.Monad.Trans
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile.Constants
import DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer
import DataSketches.Quantiles.RelativeErrorQuantile.URef


data CompactorReturn s = CompactorReturn
  { crDeltaRetItems :: !Int
  , crDeltaNominalSize :: !Int
  , crDoubleBuffer :: !(DoubleBuffer s)
  }

data ReqCompactor s (lgWeight :: Nat) = ReqCompactor 
  -- Configuration constants
  { rcRankAccuracy :: !RankAccuracy
  -- State
  , rcState :: !(URef s Word64)
  , rcLastFlip :: !(URef s Bool)
  , rcSectionSizeFlt :: !(URef s Double)
  , rcSectionSize :: !(URef s Word32)
  , rcNumSections :: !(URef s Word8)
  , rcBuffer :: !(MutVar s (DoubleBuffer s))
  }

nomCapMult :: Num a => a
nomCapMult = 2

toInt :: Integral a => a -> Int
toInt = fromIntegral 

compact :: (PrimMonad m, MonadIO m) => ReqCompactor (PrimState m) k -> m (CompactorReturn (PrimState m))
compact this = do
  startBuffSize <- getCount =<< getBuffer this
  startNominalCapacity <- getNominalCapacity this
  numSections <- readURef $ rcNumSections this
  sectionSize <- readURef $ rcSectionSize this
  state <- readURef $ rcState this
  let trailingOnes = countTrailingZeros $ complement state
      sectionsToCompact = min trailingOnes $ fromIntegral numSections
  compactionRange <- computeCompactionRange this sectionsToCompact
  let compactionStart = fromIntegral $ compactionRange .&. 0xFFFFFFFF -- low 32
      compactionEnd = fromIntegral $ compactionRange `shiftR` 32 -- hight 32
  when (compactionEnd - compactionStart >= 2) $
    error "invariant violated: compaction range too large"
  coin <- if (state .&. 1 == 1)
     then readURef $ rcLastFlip this
     else flipCoin
  writeURef (rcLastFlip this) coin
  buff <- getBuffer this
  promote <- getEvensOrOdds buff compactionStart compactionEnd coin
  trimCount buff $ startBuffSize - (compactionEnd - compactionStart)
  writeURef (rcState this) $ state + 1
  ensureEnoughSections this
  endBuffSize <- getCount buff
  promoteBuffSize <- getCount promote
  endNominalCapacity <- getNominalCapacity this
  pure $ CompactorReturn
    { crDeltaRetItems = endBuffSize - startBuffSize + promoteBuffSize
    , crDeltaNominalSize = endNominalCapacity - startNominalCapacity
    , crDoubleBuffer = promote
    }

getBuffer :: PrimMonad m => ReqCompactor (PrimState m) k -> m (DoubleBuffer (PrimState m))
getBuffer = readMutVar . rcBuffer

flipCoin :: (PrimMonad m, MonadIO m) => m Bool
flipCoin = create >>= uniform

getCoin :: PrimMonad m => ReqCompactor (PrimState m) k -> m Bool
getCoin = readURef . rcLastFlip

getLgWeight :: KnownNat n => ReqCompactor s n -> Word8
getLgWeight = fromIntegral . natVal

getNominalCapacity :: PrimMonad m => ReqCompactor (PrimState m) k -> m Int
getNominalCapacity compactor = do
  numSections <- readURef $ rcNumSections compactor
  sectionSize <- readURef $ rcSectionSize compactor
  pure $ nomCapMult * (toInt numSections) * (toInt sectionSize)

getNumSections :: PrimMonad m => ReqCompactor (PrimState m) k -> m Word8
getNumSections = readURef . rcNumSections

getSectionSizeFlt :: PrimMonad m => ReqCompactor (PrimState m) k -> m Double
getSectionSizeFlt = readURef . rcSectionSizeFlt

getState :: PrimMonad m => ReqCompactor (PrimState m) k -> m Word64
getState = readURef . rcState

merge 
  :: (PrimMonad m, s ~ PrimState m)
  => ReqCompactor (PrimState m) lgWeight 
  -> ReqCompactor (PrimState m) lgWeight 
  -> m (ReqCompactor s lgWeight)
merge this otherCompactor = do
  ensureMaxSections
  buff <- getBuffer this
  otherBuff <- getBuffer otherCompactor
  sort buff
  sort otherBuff
  otherBuffIsBigger <- (>) <$> getCount otherBuff <*> getCount buff
  finalBuff <- if otherBuffIsBigger
     then mergeSortIn otherBuff buff
     else mergeSortIn buff otherBuff
  otherState <- readURef $ rcState otherCompactor
  modifyURef (rcState this) (.|. otherState)
  writeMutVar (rcBuffer this) finalBuff
  pure this
  where
    ensureMaxSections = do
      adjusted <- ensureEnoughSections this
      -- loop until no adjustments can be made
      when adjusted ensureMaxSections

ensureEnoughSections :: PrimMonad m => ReqCompactor (PrimState m) a -> m Bool
ensureEnoughSections compactor = do
  sectionSizeFlt <- readURef $ rcSectionSizeFlt compactor
  let szf = sectionSizeFlt / sqrt2
      ne = nearestEven szf
  state <- readURef $ rcState compactor
  numSections <- readURef $ rcNumSections compactor
  sectionSize <- readURef $ rcSectionSize compactor
  if (state >= (1 `shiftL` (toInt $ numSections - 1)))
     && sectionSize > minK
     && ne >= minK
     then do
       writeURef (rcSectionSizeFlt compactor) szf
       writeURef (rcSectionSize compactor) $ fromIntegral ne
       writeURef (rcNumSections compactor) $ numSections `shiftL` 1
       pure True
     else pure False

computeCompactionRange :: PrimMonad m => ReqCompactor (PrimState m) a -> Int -> m Word64
computeCompactionRange this secsToCompact = do
  buffSize <- getCount =<< getBuffer this
  nominalCapacity <- getNominalCapacity this
  numSections <- readURef $ rcNumSections this
  sectionSize <- readURef $ rcSectionSize this
  let nonCompact :: Int
      nonCompact = floor $ fromIntegral nominalCapacity / 2 + fromIntegral (toInt numSections - secsToCompact * toInt sectionSize)
      (low, high) =
        case rcRankAccuracy this of
            HighRanksAreAccurate -> (0, fromIntegral $ buffSize - nonCompact)
            LowRanksAreAccurate -> (fromIntegral nonCompact, fromIntegral buffSize)
  pure $ low + (high `shiftL` 32)

nearestEven :: Double -> Int
nearestEven = (shiftL 1) . round . (/ 2)
