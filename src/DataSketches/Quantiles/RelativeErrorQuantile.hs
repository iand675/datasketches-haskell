module DataSketches.Quantiles.RelativeErrorQuantile
  ( ReqSketch
  , mkReqSketch
  , cumulativeDistributionFunction  
  , RankAccuracy(..)
  , rankAccuracy
  , relativeStandardError
  , maximum
  , count
  , probabilityMassFunction
  , quantile
  , quantiles
  , rank
  , rankLowerBound
  , ranks
  , rankUpperBound
  , retainedItems
  , null
  , isEstimationMode
  , isLessThanOrEqual
  , merge
  , update
  ) where

import Control.Monad (when)
import Control.Monad.Primitive
import Data.Bits (shiftL)
import qualified Data.Vector.Mutable as MVector
import Data.Primitive.MutVar
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Constants
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import DataSketches.Quantiles.RelativeErrorQuantile.Compactor (ReqCompactor)
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Compactor as Compactor
import qualified DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer as DoubleBuffer
import DataSketches.Quantiles.RelativeErrorQuantile.URef
import GHC.TypeLits
import Prelude hiding (null, minimum, maximum)

data ReqSketch s k = ReqSketch
  { sectionSize :: !Int -- ^ Referred to as k in the paper
  , rankAccuracySetting :: !RankAccuracy
  , criterion :: !Criterion
  , ltEq :: Bool
  , totalN :: !(URef s Word64)
  , minValue :: !(URef s Double)
  , maxValue :: !(URef s Double )
  , retainedItems :: !(URef s Int)
  , maxNominalCapacitiesSize :: !Int
  , aux :: !(MutVar s (Maybe ()))
  , compactors :: MVector.MVector s (ReqCompactor k s)
  }

mkReqSketch :: (PrimMonad m, 4 <= k, k <= 1024, (k `Mod` 2) ~ 0) => RankAccuracy -> m (ReqSketch (PrimState m) k)
mkReqSketch = undefined

getNumLevels :: ReqSketch s k -> Int
getNumLevels = MVector.length . compactors

getIsEmpty :: PrimMonad m => ReqSketch (PrimState m) k -> m Bool
getIsEmpty = fmap (== 0) . readURef . totalN

getN :: PrimMonad m => ReqSketch (PrimState m) k -> m Word64
getN = readURef . totalN

getRetainedItems :: PrimMonad m => ReqSketch (PrimState m) k -> m Int
getRetainedItems = readURef . retainedItems

validateSplits :: [Double] -> ()
validateSplits (s:splits) = const () $ foldl check s splits
  where
    check v lastV
      | isInfinite v = error "Values must be finite"
      | v <= lastV = error "Values must be unique and monotonically increasing"
      | otherwise = v

getCounts :: (PrimMonad m, KnownNat k) => ReqSketch (PrimState m) k -> [Double] -> m [Word64]
getCounts this values = do
  let numValues = length values
      numCompactors = MVector.length $ compactors this
      ans = take numValues $ repeat 0
  isEmpty <- getIsEmpty this
  if isEmpty
    then pure []
    else MVector.ifoldM doCount ans $ compactors this
  where
    doCount acc index compactor = do
      let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
      buff <- Compactor.getBuffer compactor
      let updateCounts buff value = do
            count_ <- DoubleBuffer.getCountWithCriterion buff (values !! index) (criterion this)
            pure $ fromIntegral value + fromIntegral count_ * wt
      mapM (updateCounts buff) acc

getPMForCDF :: (PrimMonad m, KnownNat k) => ReqSketch (PrimState m) k -> [Double] -> m [Word64]
getPMForCDF this splits = do
  pure $ validateSplits splits
  let numSplits = length splits
      numBuckets = numSplits -- + 1
  splitCounts <- getCounts this splits
  n <- getN this
  pure $ (++ [n]) $ take numBuckets $ splitCounts

cumulativeDistributionFunction :: (PrimMonad m, KnownNat k) => ReqSketch (PrimState m) k -> [Double] -> m (Maybe [Double])
cumulativeDistributionFunction this splitPoints = do
  isEmpty <- getIsEmpty this
  if (not isEmpty)
    then do
      let numBuckets = length splitPoints + 1
      buckets <- getPMForCDF this splitPoints
      n <- getN this
      pure $ Just $ (/ fromIntegral n) . fromIntegral <$> buckets
    else pure Nothing

rankAccuracy :: ReqSketch s k -> RankAccuracy
rankAccuracy = rankAccuracySetting

relativeStandardError :: ReqSketch s k -> Int -> Double -> RankAccuracy -> Int -> Double
relativeStandardError = undefined 

minimum :: PrimMonad m => ReqSketch (PrimState m) k -> m Double
minimum = readURef . minValue

maximum :: PrimMonad m => ReqSketch (PrimState m) k -> m Double
maximum = readURef . maxValue

-- not public
count :: (PrimMonad m, s ~ PrimState m, KnownNat k) => ReqSketch s k -> Double -> m Word64
count s value = fromIntegral <$> do
  empty <- null s
  if empty
    then pure 0
    else do
      let go accum compactor = do
            let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
            buf <- Compactor.getBuffer compactor
            count_ <- DoubleBuffer.getCountWithCriterion buf value (criterion s)
            pure (accum + (fromIntegral count_ * wt))
      MVector.foldM go 0 (compactors s)

probabilityMassFunction :: ReqSketch s k -> [Double] -> [Double]
probabilityMassFunction  = undefined

quantile :: ReqSketch s k -> Double -> Double
quantile = undefined

quantiles :: ReqSketch s k -> [Double] -> Double
quantiles = undefined

rank :: (PrimMonad m, s ~ PrimState m, KnownNat k) => ReqSketch s k -> Double -> m Double 
rank s value = do
  isEmpty <- null s
  if isEmpty
    then pure (0 / 0) -- NaN
    else do
      nnCount <- count s value
      total <- readURef $ totalN s
      pure (fromIntegral nnCount / fromIntegral total)



rankLowerBound :: ReqSketch s k -> Double -> Int -> Double
rankLowerBound s rank numStdDev = undefined

ranks :: (PrimMonad m, s ~ PrimState m, KnownNat k) => ReqSketch s k -> [Double] -> m [Double]
ranks s values = do
  isEmpty <- null s
  if isEmpty
    then pure []
    else do
      error "TODO"


rankUpperBound :: ReqSketch s k -> Double -> Int -> Double
rankUpperBound = undefined

-- | Renamed from isEmpty
null :: (PrimMonad m) => ReqSketch (PrimState m) k -> m Bool
null = fmap (== 0) . readURef . totalN

isEstimationMode :: ReqSketch s k -> Bool
isEstimationMode = (> 1) . getNumLevels

isLessThanOrEqual :: ReqSketch s k -> Bool
isLessThanOrEqual s = case criterion s of
  (:<) -> False
  (:<=) -> True

grow :: PrimMonad m => ReqSketch (PrimState m) k -> m ()
grow this = do
  let lgWeight = getNumLevels this
  
  undefined

compress :: PrimMonad m => ReqSketch (PrimState m) k -> m ()
compress this = undefined

merge 
  :: (PrimMonad m, s ~ PrimState m)
  => ReqSketch s k 
  -> ReqSketch s k 
  -> m (ReqSketch s k)
merge this other = do
  otherIsEmpty <- getIsEmpty other
  when (not otherIsEmpty) $ do
    let rankAccuracy = rankAccuracySetting this
        otherRankAccuracy = rankAccuracySetting other
    when (rankAccuracy /= otherRankAccuracy) $ error "Both sketches must have the same HighRankAccuracy setting."
    -- update total
    otherN <- getN other
    modifyURef (totalN this) (+ otherN)
    -- update the min and max values
    thisMin <- minimum this
    thisMax <- maximum this
    otherMin <- minimum other
    otherMax <- maximum other
    when (isNaN thisMin || otherMin < thisMin) $ do
      writeURef (minValue this) otherMin
    when (isNaN thisMax || otherMax < thisMax) $ do
      writeURef (maxValue this) otherMax
    -- grow until this has at least as many compactors as other
    undefined
  pure this

-- TODO reset?

update :: PrimMonad m => ReqSketch (PrimState m) k -> Double -> m ()
update this item = do
  when (not $ isNaN item) $ do
    isEmpty <- getIsEmpty this
    if isEmpty
       then do
         writeURef (minValue this) item
         writeURef (maxValue this) item
       else do
         min_ <- minimum this
         max_ <- maximum this
         when (item < min_) $ writeURef (minValue this) item
         when (item > max_) $ writeURef (maxValue this) item
    compactor <- MVector.read (compactors this) 0
    buff <- Compactor.getBuffer compactor
    modifyURef (retainedItems this) (+1)
    modifyURef (totalN this) (+1)
    retItems_ <-  getRetainedItems this
    when (retItems_ < maxNominalCapacitiesSize this) $ do
       DoubleBuffer.sort buff
       compress this
    writeMutVar (aux this) Nothing

-- Private pure bits

getRankLB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankLB k levels rank numStdDev hra totalN = if exactRank k levels rank hra totalN
  then rank
  else max lbRel lbFix
  where
    relative = relRseFactor / fromIntegral k * (if hra then 1.0 - rank else rank)
    fixed = fixRseFactor / fromIntegral k
    lbRel = rank - fromIntegral numStdDev * relative
    lbFix = rank - fromIntegral numStdDev * fixed

getRankUB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankUB k levels rank numStdDev hra totalN = if exactRank k levels rank hra totalN
  then rank
  else min lbRel lbFix
  where
    relative = relRseFactor / fromIntegral k * (if hra then 1.0 - rank else rank)
    fixed = fixRseFactor / fromIntegral k
    lbRel = rank + fromIntegral numStdDev * relative
    lbFix = rank + fromIntegral numStdDev * fixed
  
exactRank :: Int -> Int -> Double -> Bool -> Word64 -> Bool
exactRank k levels rank hra totalN = if levels == 1 || fromIntegral totalN <= baseCap
  then True
  else hra && rank >= 1.0 - exactRankThresh || not hra && rank <= exactRankThresh
  where
    baseCap = k * initNumberOfSections
    exactRankThresh :: Double
    exactRankThresh = fromIntegral baseCap / fromIntegral totalN

getRSE :: Int -> Double -> Bool -> Word64 -> Double
getRSE k rank hra totalN = getRankUB k 2 rank 1 hra totalN
