module DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor
  ( ReqCompactor
  , CompactorReturn (..)
  , compact
  , getBuffer
  , getCoin
  , getLgWeight
  , getNominalCapacity
  , getNumSections
  , merge
  , nearestEven
  ) where

import GHC.TypeLits
import Data.Bits ((.&.), (.|.), complement, countTrailingZeros, shiftL, shiftR)
import Data.Primitive.MutVar
import Data.Proxy
import Data.Semigroup (Semigroup)
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import System.Random.MWC (create, Variate(uniform))
import Control.Monad (when)
import Control.Monad.Trans
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.URef


data CompactorReturn s = CompactorReturn
  { crDeltaRetItems :: !Int
  , crDeltaNominalSize :: !Int
  , crDoubleBuffer :: !(DoubleBuffer s)
  }

data ReqCompactor (lgWeight :: Nat) s = ReqCompactor
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

instance TakeSnapshot (ReqCompactor k) where
  data Snapshot (ReqCompactor k) = ReqCompactorSnapshot
    { snapshotCompactorRankAccuracy :: !RankAccuracy
    , snapshotCompactorRankAccuracyState :: !Word64
    , snapshotCompactorLastFlip :: !Bool
    , snapshotCompactorSectionSizeFlt :: !Double
    , snapshotCompactorSectionSize :: !Word32
    , snapshotCompactorNumSections :: !Word8
    , snapshotCompactorBuffer :: !(Snapshot DoubleBuffer)
    }
  takeSnapshot ReqCompactor{..} = ReqCompactorSnapshot rcRankAccuracy
    <$> readURef rcState
    <*> readURef rcLastFlip
    <*> readURef rcSectionSizeFlt
    <*> readURef rcSectionSize
    <*> readURef rcNumSections
    <*> (readMutVar rcBuffer >>= takeSnapshot)

deriving instance Show (Snapshot (ReqCompactor k))

nomCapMult :: Num a => a
nomCapMult = 2

toInt :: Integral a => a -> Int
toInt = fromIntegral

compact :: (PrimMonad m) => ReqCompactor k (PrimState m) -> m (CompactorReturn (PrimState m))
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
      compactionEnd = fromIntegral $ compactionRange `shiftR` 32 -- high 32
  when (compactionEnd - compactionStart >= 2) $
    error "invariant violated: compaction range too large"
  coin <- if state .&. 1 == 1
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

getBuffer :: PrimMonad m => ReqCompactor k (PrimState m) -> m (DoubleBuffer (PrimState m))
getBuffer = readMutVar . rcBuffer

flipCoin :: (PrimMonad m) => m Bool
flipCoin = create >>= uniform

getCoin :: PrimMonad m => ReqCompactor k (PrimState m) -> m Bool
getCoin = readURef . rcLastFlip

getLgWeight :: forall k s. KnownNat k => ReqCompactor k s -> Word8
getLgWeight _ = fromIntegral (natVal (Proxy :: Proxy k))

getNominalCapacity :: PrimMonad m => ReqCompactor k (PrimState m) -> m Int
getNominalCapacity compactor = do
  numSections <- readURef $ rcNumSections compactor
  sectionSize <- readURef $ rcSectionSize compactor
  pure $ nomCapMult * toInt numSections * toInt sectionSize

getNumSections :: PrimMonad m => ReqCompactor k (PrimState m) -> m Word8
getNumSections = readURef . rcNumSections

getSectionSizeFlt :: PrimMonad m => ReqCompactor k (PrimState m) -> m Double
getSectionSizeFlt = readURef . rcSectionSizeFlt

getState :: PrimMonad m => ReqCompactor k (PrimState m) -> m Word64
getState = readURef . rcState

-- | Merge the other given compactor into this one. They both must have the
-- same @lgWeight@
merge
  :: (PrimMonad m, s ~ PrimState m)
  => ReqCompactor lgWeight (PrimState m) 
  -- ^ The compactor to merge into
  -> ReqCompactor lgWeight (PrimState m)
  -- ^ The compactor to merge from 
  -> m (ReqCompactor lgWeight s)
merge this otherCompactor = do
  ensureMaxSections
  buff <- getBuffer this
  otherBuff <- getBuffer otherCompactor
  sort buff
  sort otherBuff
  otherBuffIsBigger <- (>) <$> getCount otherBuff <*> getCount buff
  finalBuff <- if otherBuffIsBigger
     then mergeSortIn otherBuff buff >> pure otherBuff
     else mergeSortIn buff otherBuff >> pure buff
  otherState <- readURef $ rcState otherCompactor
  modifyURef (rcState this) (.|. otherState)
  writeMutVar (rcBuffer this) finalBuff
  pure this
  where
    ensureMaxSections = do
      adjusted <- ensureEnoughSections this
      -- loop until no adjustments can be made
      when adjusted ensureMaxSections

-- | Adjust the sectionSize and numSections if possible.
ensureEnoughSections 
  :: PrimMonad m 
  => ReqCompactor k (PrimState m) 
  -> m Bool
  -- ^ 'True' if the SectionSize and NumSections were adjusted.
ensureEnoughSections compactor = do
  sectionSizeFlt <- readURef $ rcSectionSizeFlt compactor
  let szf = sectionSizeFlt / sqrt2
      ne = nearestEven szf
  state <- readURef $ rcState compactor
  numSections <- readURef $ rcNumSections compactor
  sectionSize <- readURef $ rcSectionSize compactor
  if state >= (1 `shiftL` toInt (numSections - 1))
     && sectionSize > minK
     && ne >= minK
     then do
       writeURef (rcSectionSizeFlt compactor) szf
       writeURef (rcSectionSize compactor) $ fromIntegral ne
       writeURef (rcNumSections compactor) $ numSections `shiftL` 1
       pure True
     else pure False

-- | Computes the start and end indices of the compacted region
computeCompactionRange 
  :: PrimMonad m 
  => ReqCompactor k (PrimState m) 
  -> Int 
  -- ^ secsToCompact the number of contiguous sections to compact
  -> m Word64
-- ^ the start and end indices of the compacted region in compact form
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
-- | Returns the nearest even integer to the given value. Also used by test.
nearestEven 
  :: Double 
  -- ^ the given value
  -> Int
  -- ^ the nearest even integer to the given value.
nearestEven x = round (x / 2) `shiftL` 1
