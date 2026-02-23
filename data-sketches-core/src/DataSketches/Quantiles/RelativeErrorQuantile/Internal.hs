{-# LANGUAGE DeriveGeneric #-}
module DataSketches.Quantiles.RelativeErrorQuantile.Internal where

import Data.Primitive (MutVar, readMutVar)
import Data.Word
import qualified Data.Vector as Vector
import GHC.Generics
import System.Random.MWC (Gen)

import DataSketches.Core.Snapshot
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import DataSketches.Core.Internal.URef (MutableFields, readField, writeField, modifyField)
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary (ReqAuxiliary)
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor (ReqCompactor)
import Control.DeepSeq (NFData, rnf)
import Control.Monad.Primitive (PrimMonad (PrimState))
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor as Compactor
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer as DoubleBuffer
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary as Auxiliary
import Control.Exception (Exception)

-- | Mutable sketch state packed into a single 'MutableByteArray'.
--
-- All 6 scalar fields occupy 48 bytes (6 × 8-byte slots), fitting entirely
-- within one 64-byte x86-64 cache line. On the insert hot path, totalN,
-- retainedItems, maxNominalCapacitiesSize, minValue, maxValue, and sumValue
-- are all read/written — a single cache line fetch covers all of them.
--
-- Layout (element index → field):
--   0: totalN                 (Int, representing Word64)
--   1: minValue               (Double)
--   2: maxValue               (Double)
--   3: sumValue               (Double)
--   4: retainedItems          (Int)
--   5: maxNominalCapacitiesSize (Int)
data ReqSketch s = ReqSketch
  { k :: !Word32
  , rankAccuracySetting :: !RankAccuracy
  , criterion :: !Criterion
  , sketchRng :: {-# UNPACK #-} !(Gen s)
  , sketchFields :: {-# UNPACK #-} !(MutableFields s)
  , aux :: {-# UNPACK #-} !(MutVar s (Maybe ReqAuxiliary))
  , compactors :: {-# UNPACK #-} !(MutVar s (Vector.Vector (ReqCompactor s)))
  } deriving (Generic)

instance NFData (ReqSketch s) where
  rnf !_ = ()

-- Field indices
fTotalN, fMinValue, fMaxValue, fSumValue, fRetainedItems, fMaxNomCapSize :: Int
fTotalN         = 0
fMinValue       = 1
fMaxValue       = 2
fSumValue       = 3
fRetainedItems  = 4
fMaxNomCapSize  = 5

sketchFieldBytes :: Int
sketchFieldBytes = 6 * 8

-- Typed accessors

getTotalN :: PrimMonad m => ReqSketch (PrimState m) -> m Word64
getTotalN sk = do
  v <- readField (sketchFields sk) fTotalN
  pure $! fromIntegral (v :: Int)
{-# INLINE getTotalN #-}

setTotalN :: PrimMonad m => ReqSketch (PrimState m) -> Word64 -> m ()
setTotalN sk v = writeField (sketchFields sk) fTotalN (fromIntegral v :: Int)
{-# INLINE setTotalN #-}

modifyTotalN :: PrimMonad m => ReqSketch (PrimState m) -> (Word64 -> Word64) -> m ()
modifyTotalN sk f = do
  !v <- getTotalN sk
  setTotalN sk $! f v
{-# INLINE modifyTotalN #-}

getMinValue :: PrimMonad m => ReqSketch (PrimState m) -> m Double
getMinValue sk = readField (sketchFields sk) fMinValue
{-# INLINE getMinValue #-}

setMinValue :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m ()
setMinValue sk = writeField (sketchFields sk) fMinValue
{-# INLINE setMinValue #-}

getMaxValue :: PrimMonad m => ReqSketch (PrimState m) -> m Double
getMaxValue sk = readField (sketchFields sk) fMaxValue
{-# INLINE getMaxValue #-}

setMaxValue :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m ()
setMaxValue sk = writeField (sketchFields sk) fMaxValue
{-# INLINE setMaxValue #-}

getSumValue :: PrimMonad m => ReqSketch (PrimState m) -> m Double
getSumValue sk = readField (sketchFields sk) fSumValue
{-# INLINE getSumValue #-}

modifySumValue :: PrimMonad m => ReqSketch (PrimState m) -> (Double -> Double) -> m ()
modifySumValue sk f = modifyField (sketchFields sk) fSumValue f
{-# INLINE modifySumValue #-}

getRetainedItems :: PrimMonad m => ReqSketch (PrimState m) -> m Int
getRetainedItems sk = readField (sketchFields sk) fRetainedItems
{-# INLINE getRetainedItems #-}

setRetainedItems :: PrimMonad m => ReqSketch (PrimState m) -> Int -> m ()
setRetainedItems sk = writeField (sketchFields sk) fRetainedItems
{-# INLINE setRetainedItems #-}

modifyRetainedItems :: PrimMonad m => ReqSketch (PrimState m) -> (Int -> Int) -> m ()
modifyRetainedItems sk = modifyField (sketchFields sk) fRetainedItems
{-# INLINE modifyRetainedItems #-}

getMaxNomCapSize :: PrimMonad m => ReqSketch (PrimState m) -> m Int
getMaxNomCapSize sk = readField (sketchFields sk) fMaxNomCapSize
{-# INLINE getMaxNomCapSize #-}

setMaxNomCapSize :: PrimMonad m => ReqSketch (PrimState m) -> Int -> m ()
setMaxNomCapSize sk = writeField (sketchFields sk) fMaxNomCapSize
{-# INLINE setMaxNomCapSize #-}

modifyMaxNomCapSize :: PrimMonad m => ReqSketch (PrimState m) -> (Int -> Int) -> m ()
modifyMaxNomCapSize sk = modifyField (sketchFields sk) fMaxNomCapSize
{-# INLINE modifyMaxNomCapSize #-}

data ReqSketchSnapshot = ReqSketchSnapshot
    { snapshotRankAccuracySetting :: !RankAccuracy
    , snapshotCriterion :: !Criterion
    , snapshotTotalN :: !Word64
    , snapshotMinValue :: !Double
    , snapshotMaxValue :: !Double
    , snapshotRetainedItems :: !Int
    , snapshotMaxNominalCapacitiesSize :: !Int
    , snapshotCompactors :: !(Vector.Vector (Snapshot ReqCompactor))
    } deriving Show

instance TakeSnapshot ReqSketch where
  type Snapshot ReqSketch = ReqSketchSnapshot
  takeSnapshot sk = do
    tn <- getTotalN sk
    mn <- getMinValue sk
    mx <- getMaxValue sk
    ri <- getRetainedItems sk
    mc <- getMaxNomCapSize sk
    cs <- readMutVar (compactors sk) >>= mapM takeSnapshot
    pure $ ReqSketchSnapshot (rankAccuracySetting sk) (criterion sk) tn mn mx ri mc cs

getCompactors :: PrimMonad m => ReqSketch (PrimState m) -> m (Vector.Vector (ReqCompactor (PrimState m)))
getCompactors = readMutVar . compactors
{-# INLINE getCompactors #-}

computeTotalRetainedItems :: PrimMonad m => ReqSketch (PrimState m) -> m Int
computeTotalRetainedItems this = do
  cs <- getCompactors this
  Vector.foldM countBuffer 0 cs
  where
    countBuffer acc compactor = do
      buff <- Compactor.getBuffer compactor
      buffSize <- DoubleBuffer.getCount buff
      pure $! buffSize + acc

retainedItemCount :: PrimMonad m => ReqSketch (PrimState m) -> m Int
retainedItemCount = getRetainedItems
{-# INLINE retainedItemCount #-}

count :: PrimMonad m => ReqSketch (PrimState m) -> m Word64
count = getTotalN
{-# INLINE count #-}

mkAuxiliaryFromReqSketch :: PrimMonad m => ReqSketch (PrimState m) -> m ReqAuxiliary
mkAuxiliaryFromReqSketch this = do
  total <- count this
  ri <- retainedItemCount this
  cs <- getCompactors this
  Auxiliary.mkAuxiliary (rankAccuracySetting this) total ri cs

data CumulativeDistributionInvariants
  = CumulativeDistributionInvariantsSplitsAreEmpty
  | CumulativeDistributionInvariantsSplitsAreNotFinite
  | CumulativeDistributionInvariantsSplitsAreNotUniqueAndMontonicallyIncreasing
  deriving (Show, Eq)

instance Exception CumulativeDistributionInvariants
