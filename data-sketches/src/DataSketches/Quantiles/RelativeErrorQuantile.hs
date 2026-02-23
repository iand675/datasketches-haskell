{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DeriveGeneric #-}
module DataSketches.Quantiles.RelativeErrorQuantile (
  -- * Construction
    ReqSketch (criterion)
  , mkReqSketch
  -- ** Configuration settings
  , RankAccuracy(..)
  , Criterion(..)
  -- * Sketch summaries
  , count
  , null
  , sum
  , maximum
  , minimum
  , retainedItemCount
  , relativeStandardError
  , countWithCriterion
  , probabilityMassFunction
  , quantile
  , quantiles
  , rank
  , rankLowerBound
  , ranks
  , rankUpperBound
  , cumulativeDistributionFunction
  , getK
  -- * Updating the sketch
  , merge
  , insert
  , rankAccuracy
  , isEstimationMode
  , isLessThanOrEqual
  , CumulativeDistributionInvariants(..)
  ) where

import Control.Monad (when, unless)
import Control.Monad.Primitive ( PrimMonad(PrimState) )
import Data.Bits (shiftL)
import Data.Vector ((!))
import qualified Data.Vector as Vector
import Data.Primitive.MutVar
    ( modifyMutVar', newMutVar, readMutVar, writeMutVar )
import Data.Word ( Word32, Word64 )
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants
    ( fixRseFactor, initNumberOfSections, relRseFactor )
import DataSketches.Quantiles.RelativeErrorQuantile.Types
    ( Criterion(..), RankAccuracy(..) )
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor (ReqCompactor)
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary (ReqAuxiliary)
import DataSketches.Quantiles.RelativeErrorQuantile.Internal
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary as Auxiliary
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor as Compactor
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer as DoubleBuffer
import DataSketches.Core.Internal.URef (newMutableFields, writeField, MutableFields)
import Data.Maybe (isNothing)
import qualified Data.Foldable
import qualified Data.List
import Control.Exception (throw, assert)
import System.Random.MWC (create)
import Prelude hiding (sum, minimum, maximum, null)

mkReqSketch :: forall m. (PrimMonad m)
  => Word32
  -> RankAccuracy
  -> m (ReqSketch (PrimState m))
mkReqSketch k_ rank_ = do
  unless (even k_ && k_ >= 4 && k_ <= 1024) $ error "k must be divisible by 2, and satisfy 4 <= k <= 1024"
  fields <- newMutableFields sketchFieldBytes
  writeField fields fTotalN (0 :: Int)
  writeField fields fMinValue (0/0 :: Double)
  writeField fields fMaxValue (0/0 :: Double)
  writeField fields fSumValue (0 :: Double)
  writeField fields fRetainedItems (0 :: Int)
  writeField fields fMaxNomCapSize (0 :: Int)
  r <- ReqSketch k_ rank_ (:<)
    <$> create
    <*> pure fields
    <*> newMutVar Nothing
    <*> newMutVar Vector.empty
  grow r
  pure r

getAux :: PrimMonad m => ReqSketch (PrimState m) -> m (Maybe ReqAuxiliary)
getAux = readMutVar . aux

getNumLevels :: PrimMonad m => ReqSketch (PrimState m) -> m Int
getNumLevels = fmap Vector.length . getCompactors

getIsEmpty :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
getIsEmpty = fmap (== 0) . getTotalN

getK :: ReqSketch s -> Word32
getK = k

getMaxNominalCapacity :: PrimMonad m => ReqSketch (PrimState m) -> m Int
getMaxNominalCapacity = getMaxNomCapSize

validateSplits :: Monad m => [Double] -> m ()
validateSplits splits = do
  when (Data.Foldable.null splits) $
    throw CumulativeDistributionInvariantsSplitsAreEmpty
  when (any isInfinite splits || any isNaN splits) $
    throw CumulativeDistributionInvariantsSplitsAreNotFinite
  when (Data.List.nub (Data.List.sort splits) /= splits) $
    throw CumulativeDistributionInvariantsSplitsAreNotUniqueAndMontonicallyIncreasing

getCounts :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Word64]
getCounts this values = do
  cs <- getCompactors this
  let numValues = length values
      ans = replicate numValues 0
  isEmpty <- getIsEmpty this
  if isEmpty
    then pure []
    else Vector.ifoldM doCount ans cs
  where
    doCount acc index compactor = do
      let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
      buff <- Compactor.getBuffer compactor
      let updateCounts buff value = do
            count_ <- DoubleBuffer.getCountWithCriterion buff (values !! index) (criterion this)
            pure $ fromIntegral value + fromIntegral count_ * wt
      mapM (updateCounts buff) acc

getPMForCDF :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Word64]
getPMForCDF this splits = do
  () <- validateSplits splits
  let numBuckets = length splits
  splitCounts <- getCounts this splits
  n <- count this
  pure $ (++ [n]) $ take numBuckets splitCounts

cumulativeDistributionFunction
  :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m (Maybe [Double])
cumulativeDistributionFunction this splitPoints = do
  buckets <- getPMForCDF this splitPoints
  isEmpty <- getIsEmpty this
  if isEmpty
    then pure Nothing
    else do
      n <- count this
      pure $ Just $ (/ fromIntegral n) . fromIntegral <$> buckets

rankAccuracy :: ReqSketch s -> RankAccuracy
rankAccuracy = rankAccuracySetting

relativeStandardError :: Int -> Double -> RankAccuracy -> Word64 -> Double
relativeStandardError k_ rank_ hra = getRankUB k_ 2 rank_ 1 isHra
  where
    isHra = hra == HighRanksAreAccurate

minimum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
minimum = getMinValue

maximum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
maximum = getMaxValue

countWithCriterion :: (PrimMonad m, s ~ PrimState m) => ReqSketch s -> Double -> m Word64
countWithCriterion s value = fromIntegral <$> do
  empty <- null s
  if empty
    then pure 0
    else do
      cs <- getCompactors s
      let go !accum compactor = do
            let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
            buf <- Compactor.getBuffer compactor
            count_ <- DoubleBuffer.getCountWithCriterion buf value (criterion s)
            pure (accum + (fromIntegral count_ * wt))
      Vector.foldM go 0 cs

sum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
sum = getSumValue

probabilityMassFunction :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Double]
probabilityMassFunction this splitPoints = do
  isEmpty <- getIsEmpty this
  if isEmpty
     then pure []
     else do
       buckets <- fmap fromIntegral <$> getPMForCDF this splitPoints
       total <- fromIntegral <$> count this
       let computeProb (0, bucket) = bucket / total
           computeProb (i, bucket) = (prevBucket + bucket) / total
             where prevBucket = buckets !! i - 1
           probs = computeProb <$> zip [0..] buckets
       pure probs

quantile :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m Double
quantile this normRank = do
  isEmpty <- getIsEmpty this
  if isEmpty
     then pure (0/0)
     else do
       when (normRank < 0 || normRank > 1.0) $
         error $ "Normalized rank must be in the range [0.0, 1.0]: " ++ show normRank
       currAuxiliary <- getAux this
       when (isNothing currAuxiliary) $ do
         total <- count this
         ri <- retainedItemCount this
         cs <- getCompactors this
         newAuxiliary <- Auxiliary.mkAuxiliary (rankAccuracySetting this) total ri cs
         writeMutVar (aux this) (Just newAuxiliary)
       mAuxiliary <- getAux this
       case mAuxiliary of
         Just auxiliary -> pure $! Auxiliary.getQuantile auxiliary normRank $ criterion this
         Nothing -> error "invariant violated: aux is not set"

quantiles :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Double]
quantiles this normRanks = do
  isEmpty <- getIsEmpty this
  if isEmpty
     then pure []
     else mapM (quantile this) normRanks

rank :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m Double
rank s value = do
  isEmpty <- null s
  if isEmpty
    then pure (0 / 0)
    else do
      nnCount <- countWithCriterion s value
      total <- getTotalN s
      pure (fromIntegral nnCount / fromIntegral total)

rankLowerBound :: PrimMonad m => ReqSketch (PrimState m) -> Double -> Int -> m Double
rankLowerBound this r numStdDev = do
  numLevels <- getNumLevels this
  let k_ = fromIntegral $ getK this
  total <- count this
  pure $ getRankLB k_ numLevels r numStdDev (rankAccuracySetting this == HighRanksAreAccurate) total

ranks :: (PrimMonad m, s ~ PrimState m) => ReqSketch s -> [Double] -> m [Double]
ranks s values = mapM (rank s) values

rankUpperBound :: PrimMonad m => ReqSketch (PrimState m) -> Double -> Int -> m Double
rankUpperBound this r numStdDev = do
  numLevels <- getNumLevels this
  let k_ = fromIntegral $ getK this
  total <- count this
  pure $ getRankUB k_ numLevels r numStdDev (rankAccuracySetting this == HighRanksAreAccurate) total

null :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
null = getIsEmpty

isEstimationMode :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
isEstimationMode = fmap (> 1) . getNumLevels

isLessThanOrEqual :: ReqSketch s -> Bool
isLessThanOrEqual s = case criterion s of
  (:<) -> False
  (:<=) -> True

computeMaxNominalSize :: PrimMonad m => ReqSketch (PrimState m) -> m Int
computeMaxNominalSize this = do
  cs <- getCompactors this
  Vector.foldM (\acc c -> (+ acc) <$> Compactor.getNominalCapacity c) 0 cs

grow :: PrimMonad m => ReqSketch (PrimState m) -> m ()
grow this = do
  lgWeight <- fromIntegral <$> getNumLevels this
  let ra = rankAccuracySetting this
      sectionSize = getK this
  newCompactor <- Compactor.mkReqCompactor (sketchRng this) lgWeight ra sectionSize
  modifyMutVar' (compactors this) (`Vector.snoc` newCompactor)
  maxNominalCapacity <- computeMaxNominalSize this
  setMaxNomCapSize this maxNominalCapacity

compress :: PrimMonad m => ReqSketch (PrimState m) -> m ()
compress this = do
  numLevels <- getNumLevels this
  compressLoop 0 numLevels
  writeMutVar (aux this) Nothing
  where
    compressLoop height numLvls
      | height >= numLvls = pure ()
      | otherwise = do
          cs <- getCompactors this
          let compactor = cs ! height
          buffSize <- DoubleBuffer.getCount =<< Compactor.getBuffer compactor
          nominalCapacity <- Compactor.getNominalCapacity compactor
          when (buffSize >= nominalCapacity) $ do
            currentLevels <- getNumLevels this
            when (height + 1 >= currentLevels) $
              grow this
            cs' <- getCompactors this
            cReturn <- Compactor.compact compactor
            let topCompactor = cs' ! (height + 1)
            buff <- Compactor.getBuffer topCompactor
            DoubleBuffer.mergeSortIn buff $ Compactor.crDoubleBuffer cReturn
            modifyRetainedItems this (+ Compactor.crDeltaRetItems cReturn)
            modifyMaxNomCapSize this (+ Compactor.crDeltaNominalSize cReturn)
          newNumLevels <- getNumLevels this
          compressLoop (height + 1) newNumLevels

merge
  :: (PrimMonad m, s ~ PrimState m) => ReqSketch s -> ReqSketch s -> m (ReqSketch s)
merge this other = do
  otherIsEmpty <- getIsEmpty other
  unless otherIsEmpty $ do
    when (rankAccuracySetting this /= rankAccuracySetting other) $
      error "Both sketches must have the same HighRankAccuracy setting."
    otherN <- count other
    modifyTotalN this (+ otherN)
    thisMin <- minimum this
    thisMax <- maximum this
    otherMin <- minimum other
    otherMax <- maximum other
    when (isNaN thisMin || otherMin < thisMin) $ setMinValue this otherMin
    when (isNaN thisMax || otherMax > thisMax) $ setMaxValue this otherMax
    numRequiredCompactors <- getNumLevels other
    growUntil numRequiredCompactors
    thisCompactors <- getCompactors this
    otherCompactors <- getCompactors other
    Vector.zipWithM_ Compactor.merge thisCompactors otherCompactors
    maxNominalCapacity <- computeMaxNominalSize this
    totalRetainedItems <- computeTotalRetainedItems this
    setMaxNomCapSize this maxNominalCapacity
    setRetainedItems this totalRetainedItems
    when (totalRetainedItems >= maxNominalCapacity) $ compress this
    maxNominalCapacity' <- getMaxNomCapSize this
    totalRetainedItems' <- getRetainedItems this
    assert (totalRetainedItems' < maxNominalCapacity') $
      writeMutVar (aux this) Nothing
  pure this
  where
    growUntil target = do
      numCompactors <- getNumLevels this
      when (numCompactors < target) $ do
        grow this
        growUntil target

insert :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m ()
insert this item = do
  unless (isNaN item) $ do
    isEmpty <- getIsEmpty this
    if isEmpty
       then do
         setMinValue this item
         setMaxValue this item
       else do
         min_ <- minimum this
         max_ <- maximum this
         when (item < min_) $ setMinValue this item
         when (item > max_) $ setMaxValue this item
    compactor <- Vector.head <$> getCompactors this
    buff <- Compactor.getBuffer compactor
    DoubleBuffer.append buff item
    modifyRetainedItems this (+1)
    modifyTotalN this (+1)
    modifySumValue this (+ item)
    retItems <- retainedItemCount this
    maxNominalCapacity <- getMaxNominalCapacity this
    when (retItems >= maxNominalCapacity) $ do
      DoubleBuffer.sort buff
      compress this
    writeMutVar (aux this) Nothing

-- Private pure bits

getRankLB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankLB k_ levels r numStdDev hra totalN_ = if exactRank k_ levels r hra totalN_
  then r
  else max lbRel lbFix
  where
    relative = relRseFactor / fromIntegral k_ * (if hra then 1.0 - r else r)
    fixed = fixRseFactor / fromIntegral k_
    lbRel = r - fromIntegral numStdDev * relative
    lbFix = r - fromIntegral numStdDev * fixed

getRankUB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankUB k_ levels r numStdDev hra totalN_ = if exactRank k_ levels r hra totalN_
  then r
  else min ubRel ubFix
  where
    relative = relRseFactor / fromIntegral k_ * (if hra then 1.0 - r else r)
    fixed = fixRseFactor / fromIntegral k_
    ubRel = r + fromIntegral numStdDev * relative
    ubFix = r + fromIntegral numStdDev * fixed

exactRank :: Int -> Int -> Double -> Bool -> Word64 -> Bool
exactRank k_ levels r hra totalN_ =
  (levels == 1 || fromIntegral totalN_ <= baseCap)
  || (hra && r >= 1.0 - exactRankThresh || not hra && r <= exactRankThresh)
  where
    baseCap = k_ * initNumberOfSections
    exactRankThresh :: Double
    exactRankThresh = fromIntegral baseCap / fromIntegral totalN_
