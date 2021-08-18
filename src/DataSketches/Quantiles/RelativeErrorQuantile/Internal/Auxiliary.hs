module DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary
  ( ReqAuxiliary
  , mkAuxiliary
  , getQuantile
  ) where

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
import qualified Data.Vector.Unboxed as U
import Control.Monad.ST

data ReqAuxiliary = ReqAuxiliary
  { raWeightedItems :: !(U.Vector (Double, Word64))
  , raHighRankAccuracy :: !RankAccuracy
  , raSize :: !Word64
  }
  deriving (Eq)

data MReqAuxiliary s = MReqAuxiliary
  { mraWeightedItems :: !(MutVar s (MUVector.MVector s (Double, Word64)))
  , mraHighRankAccuracy :: !RankAccuracy
  , mraSize :: !Word64
  }

mkAuxiliary :: (PrimMonad m, s ~ PrimState m, KnownNat k) => RankAccuracy -> Word64 -> Int -> Vector.Vector (ReqCompactor k s) -> m ReqAuxiliary
mkAuxiliary rankAccuracy totalN retainedItems compactors = do
  items <- newMutVar =<< MUVector.new retainedItems
  let this = MReqAuxiliary
        { mraWeightedItems = items
        , mraHighRankAccuracy = rankAccuracy
        , mraSize = totalN
        }
  Vector.foldM_ (mergeBuffers this) 0 compactors
  createCumulativeWeights this
  dedup this
  items' <- U.unsafeFreeze =<< readMutVar items
  pure ReqAuxiliary
    { raWeightedItems = items'
    , raHighRankAccuracy = rankAccuracy
    , raSize = totalN
    }
  where
    mergeBuffers this auxCount compactor = do
      buff <- Compactor.getBuffer compactor
      buffSize <-  DoubleBuffer.getCount buff
      let lgWeight = Compactor.getLgWeight compactor
          weight = 1 `shiftL` fromIntegral lgWeight
      mergeSortIn this buff weight auxCount
      pure $ auxCount + buffSize

getWeightedItems :: PrimMonad m => MReqAuxiliary (PrimState m) -> m (MUVector.MVector (PrimState m) (Double, Word64))
getWeightedItems = readMutVar . mraWeightedItems

getItems :: PrimMonad m => MReqAuxiliary (PrimState m) -> m (MUVector.MVector (PrimState m) Double)
getItems = fmap (fst . MUVector.unzip) . getWeightedItems

getWeights :: PrimMonad m => MReqAuxiliary (PrimState m) -> m (MUVector.MVector (PrimState m) Word64)
getWeights = fmap (snd . MUVector.unzip) . getWeightedItems

getQuantile :: ReqAuxiliary -> Double -> Bool -> Double
getQuantile this normalRank ltEq = fst (weightedItems U.! ix)
  where
    ix = runST $ do
      v <- U.unsafeThaw $ snd $ U.unzip weightedItems
      binarySearchPBounds pred v 0 (weightsSize - 1)
    weightedItems = raWeightedItems this
    weightsSize = U.length weightedItems
    rank = normalRank * fromIntegral (raSize this)
    comparator = if ltEq then (>= rank) else (> rank)
    pred = comparator . fromIntegral

createCumulativeWeights :: PrimMonad m => MReqAuxiliary (PrimState m) -> m ()
createCumulativeWeights this = do
  items <- getItems this
  weights <- getWeights this
  let size = MUVector.length items
  MUVector.iforM_ weights $ \i weight -> do
    when (i > 0) $ do
      prevWeight <- MUVector.read weights (i - 1)
      MUVector.write weights i (weight + prevWeight)
  lastWeight <- MUVector.read weights (size - 1)
  when (lastWeight /= mraSize this) $ do
    error "invariant violated: lastWeight does not equal raSize"

dedup :: PrimMonad m => MReqAuxiliary (PrimState m) -> m ()
dedup this = do
  weightedItems <- getWeightedItems this
  let size = MUVector.length weightedItems
  weightedItemsB <- MUVector.new size
  bi <- doDedup weightedItems size weightedItemsB 0 0
  writeMutVar (mraWeightedItems this) $ MUVector.slice 0 bi weightedItemsB
  where
    doDedup weightedItems itemsSize weightedItemsB = go
      where go !i !bi
              | i >= itemsSize = pure bi
              | otherwise = do
                let j = i + 1
                    hidup = j
                    countDups !j !hidup = do
                      (itemI, _) <- MUVector.read weightedItems i
                      (itemJ, _) <- MUVector.read weightedItems j
                      if (j < itemsSize) && (itemI == itemJ)
                         then countDups (j + 1) (hidup + 1)
                         else pure (j, hidup)
                (j', hidup') <- countDups j hidup
                if j' - i == 1 -- no dups
                   then do
                     (item, weight) <- MUVector.read weightedItems i
                     MUVector.write weightedItemsB bi (item, weight)
                     go (i + 1) (bi + 1)
                   else do
                     (item, weight) <- MUVector.read weightedItems hidup'
                     MUVector.write weightedItemsB bi (item, weight)
                     go j' (bi + 1)

mergeSortIn :: PrimMonad m => MReqAuxiliary (PrimState m) -> DoubleBuffer (PrimState m) -> Word64 -> Int -> m ()
mergeSortIn this otherBuff defaultWeight auxCount = do
  DoubleBuffer.sort otherBuff
  weightedItems <- getWeightedItems this
  otherItems <- DoubleBuffer.getVector otherBuff
  otherBuffSize <- DoubleBuffer.getCount otherBuff
  otherBuffCapacity <- DoubleBuffer.getCapacity otherBuff
  let totalSize = otherBuffSize + auxCount
      height = case mraHighRankAccuracy this of
        HighRanksAreAccurate -> otherBuffCapacity - 1
        LowRanksAreAccurate -> otherBuffSize - 1
  merge weightedItems otherItems (auxCount - 1) (otherBuffSize - 1) height totalSize
  where
    merge weightedItems otherItems = go
      where
        go !i !j !h !k
          | k <= 0 = pure ()
          | i >= 0 && j >= 0 = do
            (item, weight) <- MUVector.read weightedItems i
            otherItem <- MUVector.read otherItems h
            if item >= otherItem
               then do
                 MUVector.write weightedItems k (item, weight)
                 go (i - 1) j h (k - 1)
               else do
                 MUVector.write weightedItems k (otherItem, defaultWeight)
                 go i (j - 1) (h - 1) (k - 1)
          | i >= 0 = do
            MUVector.read weightedItems i >>= MUVector.write weightedItems k
            go (i - 1) j h (k - 1)
          | j >= 0 = do
            otherItem <- MUVector.read otherItems h
            MUVector.write weightedItems k (otherItem, defaultWeight)
            go i (j - 1) (h - 1) (k - 1)
          | otherwise = pure ()
