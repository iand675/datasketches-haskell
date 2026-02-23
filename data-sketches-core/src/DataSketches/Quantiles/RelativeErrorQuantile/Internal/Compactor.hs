module DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor
  ( ReqCompactor
  , mkReqCompactor
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

import Data.Bits ((.&.), (.|.), complement, countTrailingZeros, shiftL, shiftR)
import Data.Primitive.MutVar
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import System.Random.MWC (Variate(uniform), Gen)
import Control.Exception (assert)
import Control.Monad (when)
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer
import DataSketches.Core.Internal.URef (MutableFields, newMutableFields, readField, writeField, modifyField)
import DataSketches.Core.Snapshot

data CompactorReturn s = CompactorReturn
  { crDeltaRetItems :: {-# UNPACK #-} !Int
  , crDeltaNominalSize :: {-# UNPACK #-} !Int
  , crDoubleBuffer :: {-# UNPACK #-} !(DoubleBuffer s)
  }

-- | Mutable compactor state packed into a single 'MutableByteArray'.
--
-- All scalar fields are stored at 8-byte-aligned offsets in one contiguous
-- allocation. On x86-64 (64-byte cache lines), the entire 40-byte block
-- fits in a single cache line, so reading state + numSections + sectionSize
-- during compact (the hot path) never causes a second cache miss.
--
-- Layout (byte offset → field):
--   0:  state          (Word64) — compaction counter, read/written every compact
--   8:  sectionSizeFlt (Double) — current section size as float
--  16:  sectionSize    (Int)    — current section size
--  24:  numSections    (Int)    — current number of sections
--  32:  lastFlip       (Int)    — last coin flip result (0 or 1)
data ReqCompactor s = ReqCompactor
  { rcRankAccuracy :: !RankAccuracy
  , rcLgWeight :: {-# UNPACK #-} !Word8
  , rcRng :: {-# UNPACK #-} !(Gen s)
  , rcFields :: {-# UNPACK #-} !(MutableFields s)
  , rcBuffer :: {-# UNPACK #-} !(MutVar s (DoubleBuffer s))
  }

-- Field indices (element index into the Int/Double-sized slots of MutableByteArray)
-- All stored as 8-byte values for uniform alignment.
fState, fSectionSizeFlt, fSectionSize, fNumSections, fLastFlip :: Int
fState          = 0  -- Word64, but stored via Int-sized readField/writeField
fSectionSizeFlt = 1  -- Double
fSectionSize    = 2  -- Int
fNumSections    = 3  -- Int
fLastFlip       = 4  -- Int (0 or 1)

fieldBytes :: Int
fieldBytes = 5 * 8  -- 40 bytes total, fits in one 64-byte cache line

-- Typed accessors. These read/write the MutableByteArray at the right element
-- index, relying on Prim instances for Int, Double, Word64.

getStateField :: PrimMonad m => ReqCompactor (PrimState m) -> m Word64
getStateField rc = do
  v <- readField (rcFields rc) fState
  pure $! fromIntegral (v :: Int)
{-# INLINE getStateField #-}

setStateField :: PrimMonad m => ReqCompactor (PrimState m) -> Word64 -> m ()
setStateField rc v = writeField (rcFields rc) fState (fromIntegral v :: Int)
{-# INLINE setStateField #-}

modifyStateField :: PrimMonad m => ReqCompactor (PrimState m) -> (Word64 -> Word64) -> m ()
modifyStateField rc f = do
  !v <- getStateField rc
  setStateField rc $! f v
{-# INLINE modifyStateField #-}

getSectionSizeFltField :: PrimMonad m => ReqCompactor (PrimState m) -> m Double
getSectionSizeFltField rc = readField (rcFields rc) fSectionSizeFlt
{-# INLINE getSectionSizeFltField #-}

setSectionSizeFltField :: PrimMonad m => ReqCompactor (PrimState m) -> Double -> m ()
setSectionSizeFltField rc = writeField (rcFields rc) fSectionSizeFlt
{-# INLINE setSectionSizeFltField #-}

getSectionSizeField :: PrimMonad m => ReqCompactor (PrimState m) -> m Int
getSectionSizeField rc = readField (rcFields rc) fSectionSize
{-# INLINE getSectionSizeField #-}

setSectionSizeField :: PrimMonad m => ReqCompactor (PrimState m) -> Int -> m ()
setSectionSizeField rc = writeField (rcFields rc) fSectionSize
{-# INLINE setSectionSizeField #-}

getNumSectionsField :: PrimMonad m => ReqCompactor (PrimState m) -> m Int
getNumSectionsField rc = readField (rcFields rc) fNumSections
{-# INLINE getNumSectionsField #-}

setNumSectionsField :: PrimMonad m => ReqCompactor (PrimState m) -> Int -> m ()
setNumSectionsField rc = writeField (rcFields rc) fNumSections
{-# INLINE setNumSectionsField #-}

getLastFlipField :: PrimMonad m => ReqCompactor (PrimState m) -> m Bool
getLastFlipField rc = (/= (0 :: Int)) <$> readField (rcFields rc) fLastFlip
{-# INLINE getLastFlipField #-}

setLastFlipField :: PrimMonad m => ReqCompactor (PrimState m) -> Bool -> m ()
setLastFlipField rc b = writeField (rcFields rc) fLastFlip (if b then 1 :: Int else 0)
{-# INLINE setLastFlipField #-}

data ReqCompactorSnapshot = ReqCompactorSnapshot
    { snapshotCompactorRankAccuracy :: !RankAccuracy
    , snapshotCompactorRankAccuracyState :: !Word64
    , snapshotCompactorLastFlip :: !Bool
    , snapshotCompactorSectionSizeFlt :: !Double
    , snapshotCompactorSectionSize :: !Word32
    , snapshotCompactorNumSections :: !Word8
    , snapshotCompactorBuffer :: !(Snapshot DoubleBuffer)
    } deriving (Show)

instance TakeSnapshot ReqCompactor where
  type Snapshot ReqCompactor = ReqCompactorSnapshot
  takeSnapshot rc = do
    st <- getStateField rc
    fl <- getLastFlipField rc
    szf <- getSectionSizeFltField rc
    sz <- getSectionSizeField rc
    ns <- getNumSectionsField rc
    buf <- readMutVar (rcBuffer rc) >>= takeSnapshot
    pure $ ReqCompactorSnapshot
      (rcRankAccuracy rc) st fl szf (fromIntegral sz) (fromIntegral ns) buf

mkReqCompactor
  :: PrimMonad m
  => Gen (PrimState m)
  -> Word8
  -> RankAccuracy
  -> Word32
  -> m (ReqCompactor (PrimState m))
mkReqCompactor g lgWeight rankAccuracy sectionSize = do
  let nominalCapacity = fromIntegral $ nomCapMulti * initNumberOfSections * sectionSize
  buff <- mkBuffer (nominalCapacity * 2) nominalCapacity (rankAccuracy == HighRanksAreAccurate)
  fields <- newMutableFields fieldBytes
  writeField fields fState (0 :: Int)
  writeField fields fSectionSizeFlt (fromIntegral sectionSize :: Double)
  writeField fields fSectionSize (fromIntegral sectionSize :: Int)
  writeField fields fNumSections (fromIntegral initNumberOfSections :: Int)
  writeField fields fLastFlip (0 :: Int)
  bufMv <- newMutVar buff
  pure $ ReqCompactor rankAccuracy lgWeight g fields bufMv

nomCapMult :: Int
nomCapMult = 2

compact :: PrimMonad m => ReqCompactor (PrimState m) -> m (CompactorReturn (PrimState m))
compact this = do
  startBuffSize <- getCount =<< getBuffer this
  startNominalCapacity <- getNominalCapacity this
  numSections <- getNumSectionsField this
  sectionSize <- getSectionSizeField this
  state <- getStateField this
  let trailingOnes = succ $ countTrailingZeros $ complement state
      sectionsToCompact = min trailingOnes numSections
  (compactionStart, compactionEnd) <- computeCompactionRange this sectionsToCompact
  assert (compactionEnd - compactionStart >= 2) $ do
    coin <- if state .&. 1 == 1
      then fmap not $ getLastFlipField this
      else flipCoin this
    setLastFlipField this coin
    buff <- getBuffer this
    promote <- getEvensOrOdds buff compactionStart compactionEnd coin
    trimCount buff $ startBuffSize - (compactionEnd - compactionStart)
    modifyStateField this (+ 1)
    ensureEnoughSections this
    endBuffSize <- getCount buff
    promoteBuffSize <- getCount promote
    endNominalCapacity <- getNominalCapacity this
    pure $ CompactorReturn
      { crDeltaRetItems = endBuffSize - startBuffSize + promoteBuffSize
      , crDeltaNominalSize = endNominalCapacity - startNominalCapacity
      , crDoubleBuffer = promote
      }

getLgWeight :: ReqCompactor s -> Word8
getLgWeight = rcLgWeight

getBuffer :: PrimMonad m => ReqCompactor (PrimState m) -> m (DoubleBuffer (PrimState m))
getBuffer = readMutVar . rcBuffer
{-# INLINE getBuffer #-}

flipCoin :: PrimMonad m => ReqCompactor (PrimState m) -> m Bool
flipCoin = uniform . rcRng
{-# INLINE flipCoin #-}

getCoin :: PrimMonad m => ReqCompactor (PrimState m) -> m Bool
getCoin = getLastFlipField

getNominalCapacity :: PrimMonad m => ReqCompactor (PrimState m) -> m Int
getNominalCapacity compactor = do
  numSections <- getNumSectionsField compactor
  sectionSize <- getSectionSizeField compactor
  pure $! nomCapMult * numSections * sectionSize
{-# INLINE getNominalCapacity #-}

getNumSections :: PrimMonad m => ReqCompactor (PrimState m) -> m Word8
getNumSections rc = fromIntegral <$> getNumSectionsField rc

merge
  :: (PrimMonad m, s ~ PrimState m)
  => ReqCompactor (PrimState m)
  -> ReqCompactor (PrimState m)
  -> m (ReqCompactor s)
merge this otherCompactor = assert (rcLgWeight this == rcLgWeight otherCompactor) $ do
  otherState <- getStateField otherCompactor
  modifyStateField this (.|. otherState)
  ensureMaxSections

  buff <- getBuffer this
  sort buff

  otherBuff <- getBuffer otherCompactor
  sort otherBuff

  otherBuffIsBigger <- (>) <$> getCount otherBuff <*> getCount buff
  if otherBuffIsBigger
     then do
        otherBuff' <- copyBuffer otherBuff
        mergeSortIn otherBuff' buff
        writeMutVar (rcBuffer this) otherBuff'
     else mergeSortIn buff otherBuff
  pure this
  where
    ensureMaxSections = do
      adjusted <- ensureEnoughSections this
      when adjusted ensureMaxSections

ensureEnoughSections
  :: PrimMonad m
  => ReqCompactor (PrimState m)
  -> m Bool
ensureEnoughSections compactor = do
  sectionSizeFlt <- getSectionSizeFltField compactor
  let szf = sectionSizeFlt / sqrt2
      ne = nearestEven szf
  state <- getStateField compactor
  numSections <- getNumSectionsField compactor
  sectionSize <- getSectionSizeField compactor
  if state >= (1 `shiftL` (numSections - 1))
     && sectionSize > minK
     && ne >= minK
     then do
       setSectionSizeFltField compactor szf
       setSectionSizeField compactor ne
       setNumSectionsField compactor (numSections `shiftL` 1)
       buf <- getBuffer compactor
       nomCapacity <- getNominalCapacity compactor
       ensureCapacity buf (2 * nomCapacity)
       pure True
     else pure False

computeCompactionRange
  :: PrimMonad m
  => ReqCompactor (PrimState m)
  -> Int
  -> m (Int, Int)
computeCompactionRange this secsToCompact = do
  buffSize <- getCount =<< getBuffer this
  nominalCapacity <- getNominalCapacity this
  numSections <- getNumSectionsField this
  sectionSize <- getSectionSizeField this
  let nonCompact = (nominalCapacity `div` 2) + (numSections - secsToCompact) * sectionSize
      nonCompact' = if (buffSize - nonCompact) .&. 1 == 1 then nonCompact - 1 else nonCompact
  pure $ case rcRankAccuracy this of
    HighRanksAreAccurate -> (0, buffSize - nonCompact')
    LowRanksAreAccurate -> (nonCompact', buffSize)

nearestEven :: Double -> Int
nearestEven x = round (x / 2) `shiftL` 1
