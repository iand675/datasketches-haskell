module DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary where

import GHC.TypeLits
import Control.Monad (when)
import Control.Monad.Primitive
import Data.Bits (shiftL)
import Data.Word
import Data.Primitive.MutVar
import Data.Vector.Algorithms.Search
import qualified Data.Vector as Vector
import qualified Data.Vector.Unboxed.Mutable as MUVector
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor (ReqCompactor)
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor as Compactor
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer (DoubleBuffer)
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer as DoubleBuffer

data ReqAuxiliary s = ReqAuxiliary
  { raItems :: !(MutVar s (MUVector.MVector s Double))
  , raWeights :: !(MutVar s (MUVector.MVector s Word64))
  , raHighRankAccuracy :: !Bool
  , raSize :: !Word64
  }
  deriving (Eq)

mkAuxiliary :: (PrimMonad m, s ~ PrimState m, KnownNat k) => RankAccuracy -> Word64 -> Int -> Vector.Vector (ReqCompactor k s) -> m (ReqAuxiliary s)
mkAuxiliary rankAccuracy totalN retainedItems compactors = do
  items <- newMutVar =<< MUVector.new retainedItems
  weights <- newMutVar =<< MUVector.new retainedItems
  let this = ReqAuxiliary
        { raItems = items
        , raWeights = weights
        , raHighRankAccuracy = rankAccuracy == HighRanksAreAccurate
        , raSize = totalN
        }
  Vector.foldM (mergeBuffers this) 0 compactors
  createCumulativeWeights this
  dedup this
  pure this
  where
    mergeBuffers this auxCount compactor = do
      buff <- Compactor.getBuffer compactor
      buffSize <-  DoubleBuffer.getCount buff
      let lgWeight = Compactor.getLgWeight compactor
          weight = 1 `shiftL` fromIntegral lgWeight
      mergeSortIn this buff weight auxCount
      pure $ auxCount + buffSize

getItems :: PrimMonad m => ReqAuxiliary (PrimState m) -> m (MUVector.MVector (PrimState m) Double)
getItems = readMutVar . raItems

getWeights :: PrimMonad m => ReqAuxiliary (PrimState m) -> m (MUVector.MVector (PrimState m) Word64)
getWeights = readMutVar . raWeights

getQuantile :: PrimMonad m => ReqAuxiliary (PrimState m) -> Double -> Bool -> m Double
getQuantile this normalRank ltEq = do
  items <- getItems this
  weights <- getWeights this
  let weightsSize = MUVector.length weights
      rank = normalRank * (fromIntegral $ raSize this)
      comparator = if ltEq then (>= rank) else (> rank)
      pred = comparator . fromIntegral
  ix <- binarySearchPBounds pred weights 0 (weightsSize - 1)
  item <- MUVector.read items ix
  pure item

createCumulativeWeights :: PrimMonad m => ReqAuxiliary (PrimState m) -> m ()
createCumulativeWeights this = do
  items <- getItems this
  weights <- getWeights this
  let size = MUVector.length items
  MUVector.iforM_ weights $ \i weight -> do
    when (i > 0) $ do
      prevWeight <- MUVector.read weights (i - 1)
      MUVector.write weights i (weight + prevWeight)
  lastWeight <- MUVector.read weights (size - 1)
  when (lastWeight /= raSize this) $ do
    error "invariant violated: lastWeight does not equal raSize"

dedup :: PrimMonad m => ReqAuxiliary (PrimState m) -> m ()
dedup this = do
  items <- getItems this
  weights <- getWeights this
  let size = MUVector.length items
  itemsB <- MUVector.new size
  weightsB <- MUVector.new size
  bi <- doDedup items weights size itemsB weightsB 0 0
  writeMutVar (raItems this) $ MUVector.slice 0 bi itemsB
  writeMutVar (raWeights this) $ MUVector.slice 0 bi weightsB
  where
    doDedup items weights itemsSize itemsB weightsB = go
      where go !i !bi
              | i >= itemsSize = pure bi
              | otherwise = do
                let j = i + 1
                    hidup = j
                    countDups !j !hidup = do
                      itemI <- MUVector.read items i
                      itemJ <- MUVector.read items j
                      if (j < itemsSize) && (itemI == itemJ)
                         then countDups (j + 1) (hidup + 1)
                         else pure (j, hidup)
                (j', hidup') <- countDups j hidup
                if (j' - i == 1) -- no dups
                   then do
                     item <- MUVector.read items i
                     weight <- MUVector.read weights i
                     MUVector.write itemsB bi item
                     MUVector.write weightsB bi weight
                     go (i + 1) (bi + 1)
                   else do
                     item <- MUVector.read items hidup'
                     weight <- MUVector.read weights hidup'
                     MUVector.write itemsB bi item
                     MUVector.write weightsB bi weight
                     go j' (bi + 1)

mergeSortIn :: PrimMonad m => ReqAuxiliary (PrimState m) -> (DoubleBuffer (PrimState m)) -> Word64 -> Int -> m ()
mergeSortIn this otherBuff defaultWeight auxCount = do
  DoubleBuffer.sort otherBuff
  items <- getItems this
  weights <- getWeights this
  otherItems <- DoubleBuffer.getVector otherBuff
  otherBuffSize <- DoubleBuffer.getCount otherBuff
  otherBuffCapacity <- DoubleBuffer.getCapacity otherBuff
  let totalSize = otherBuffSize + auxCount
      height = if (raHighRankAccuracy this)
                  then otherBuffCapacity - 1
                  else otherBuffSize - 1
  merge items weights otherItems (auxCount - 1) (otherBuffSize - 1) height totalSize
  where
    merge items weights otherItems = go
      where
        go !i !j !h !k
          | k <= 0 = pure ()
          | i >= 0 && j >= 0 = do
            item <- MUVector.read items i
            weight <- MUVector.read weights i
            otherItem <- MUVector.read otherItems h
            if (item >= otherItem)
               then do
                 MUVector.write items k item
                 MUVector.write weights k weight
                 go (i - 1) j h (k - 1)
               else do
                 MUVector.write items k otherItem
                 MUVector.write weights k defaultWeight
                 go i (j - 1) (h - 1) (k - 1)
          | i >= 0 = do
            item <- MUVector.read items i
            weight <- MUVector.read weights i
            MUVector.write items k item
            MUVector.write weights k weight
            go (i - 1) j h (k - 1)
          | j >= 0 = do
            otherItem <- MUVector.read otherItems h
            MUVector.write items k otherItem
            MUVector.write weights k defaultWeight
            go i (j - 1) (h - 1) (k - 1)
          | otherwise = pure ()
