-- | The Relative Error Quantile (REQ) sketch provides extremely high accuracy at a chosen end of the rank domain. 
-- This is best illustrated with some rank domain accuracy plots that compare the KLL quantiles sketch to the REQ sketch.
--
-- This first plot illustrates the typical error behavior of the KLL sketch (also the quantiles/DoublesSketch). 
-- The error is flat for all ranks (0, 1). The green and yellow lines correspond to +/- one RSE at 68% confidence; 
-- the blue and red lines, +/- two RSE at 95% confidence; and, the purple and brown lines +/- 3 RSE at 99% confidence. 
-- The reason all the curves pinch at 0 and 1.0, is because the sketch knows with certainty that a request for a quantile at 
-- rank = 0 is the minimum value of the stream; and a request for a quantiles at rank = 1.0, is the maximum value of the stream. 
-- Both of which the sketch tracks.
--
-- ![KLL Gaussian Error Quantiles](docs/images/KllErrorK100SL11.png)
--
-- The next plot is the exact same data and queries fed to the REQ sketch set for High Rank Accuracy (HRA) mode. 
-- In this plot, starting at a rank of about 0.3, the contour lines start converging and actually reach zero error at 
-- rank 1.0. Therefore the error (the inverse of accuracy) is relative to the requested rank, thus the name of the sketch. 
-- This means that the user can perform getQuantile(rank) queries, where rank = .99999 and get accurate results.
--
-- ![ReqSketch Gaussian Error Quantiles - HighRankAccuracy](docs/images/ReqErrorHraK12SL11_LT.png)
--
-- This next plot is also the same data and queries, except the REQ sketch was configured for Low Rank Accuracy (LRA). In this case the user can perform getQuantiles(rank) queries, where rank = .00001 and get accurate results.
--
-- ![ReqSketch Gaussian Error Quantiles - LowRankAccuracy](docs/images/ReqErrorLraK12SL11_LE.png)

{-# LANGUAGE ConstraintKinds #-}
module DataSketches.Quantiles.RelativeErrorQuantile
  ( ReqSketch
  , ValidK
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

import Control.Monad (when, unless)
import Control.Monad.Primitive
import Control.Monad.Trans
import Data.Bits (shiftL)
import Data.Vector ((!))
import qualified Data.Vector as Vector
import Data.Primitive.MutVar
import Data.Word
import Data.Foldable (for_)
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor (ReqCompactor)
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor as Compactor
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer as DoubleBuffer
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.URef
import GHC.TypeLits
import Prelude hiding (null, minimum, maximum)

data ReqSketch k s = ReqSketch
  { rankAccuracySetting :: !RankAccuracy
  , criterion :: !Criterion
  , totalN :: !(URef s Word64)
  , minValue :: !(URef s Double)
  , maxValue :: !(URef s Double )
  , retainedItems :: !(URef s Int)
  , maxNominalCapacitiesSize :: !(URef s Int)
  , aux :: !(MutVar s (Maybe ()))
  , compactors :: !(MutVar s (Vector.Vector (ReqCompactor k s)))
  }

instance TakeSnapshot (ReqSketch k) where
  data Snapshot (ReqSketch k) = ReqSketchSnapshot
    { snapshotRankAccuracySetting :: !RankAccuracy
    , snapshotCriterion :: !Criterion
    , snapshotTotalN :: !Word64
    , snapshotMinValue :: !Double
    , snapshotMaxValue :: !Double
    , snapshotRetainedItems :: !Int
    , snapshotMaxNominalCapacitiesSize :: !Int
    -- , aux :: !(MutVar s (Maybe ()))
    , snapshotCompactors :: !(Vector.Vector (Snapshot (ReqCompactor k)))
    }
  takeSnapshot ReqSketch{..} = ReqSketchSnapshot rankAccuracySetting criterion
    <$> readURef totalN
    <*> readURef minValue
    <*> readURef maxValue
    <*> readURef retainedItems
    <*> readURef maxNominalCapacitiesSize
    <*> (readMutVar compactors >>= mapM takeSnapshot)

deriving instance Show (Snapshot (ReqSketch k))

type ValidK k = (4 <= k, k <= 1024, (k `Mod` 2) ~ 0)

mkReqSketch :: forall k m. (PrimMonad m, ValidK k) => RankAccuracy -> m (ReqSketch k (PrimState m))
mkReqSketch rank = do
  r <- ReqSketch rank (:<)
    <$> newURef 0
    <*> newURef (0 / 0)
    <*> newURef (0 / 0)
    <*> newURef 0
    <*> newURef 0
    <*> newMutVar Nothing
    <*> newMutVar Vector.empty
  grow r
  pure r



getCompactors :: PrimMonad m => ReqSketch k (PrimState m) -> m (Vector.Vector (ReqCompactor k (PrimState m)))
getCompactors = readMutVar . compactors

getNumLevels :: PrimMonad m => ReqSketch k (PrimState m) -> m Int
getNumLevels = fmap Vector.length . getCompactors

getIsEmpty :: PrimMonad m => ReqSketch k (PrimState m) -> m Bool
getIsEmpty = fmap (== 0) . readURef . totalN

getN :: PrimMonad m => ReqSketch k (PrimState m) -> m Word64
getN = readURef . totalN

getRetainedItems :: PrimMonad m => ReqSketch k (PrimState m) -> m Int
getRetainedItems = readURef . retainedItems

getMaxNominalCapacity :: PrimMonad m => ReqSketch k (PrimState m) -> m Int
getMaxNominalCapacity = readURef . maxNominalCapacitiesSize

validateSplits :: [Double] -> ()
validateSplits (s:splits) = const () $ foldl check s splits
  where
    check v lastV
      | isInfinite v = error "Values must be finite"
      | v <= lastV = error "Values must be unique and monotonically increasing"
      | otherwise = v

getCounts :: (PrimMonad m, KnownNat k) => ReqSketch k (PrimState m) -> [Double] -> m [Word64]
getCounts this values = do
  compactors <- getCompactors this
  let numValues = length values
      numCompactors = Vector.length compactors
      ans = take numValues $ repeat 0
  isEmpty <- getIsEmpty this
  if isEmpty
    then pure []
    else Vector.ifoldM doCount ans $ compactors
  where
    doCount acc index compactor = do
      let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
      buff <- Compactor.getBuffer compactor
      let updateCounts buff value = do
            count_ <- DoubleBuffer.getCountWithCriterion buff (values !! index) (criterion this)
            pure $ fromIntegral value + fromIntegral count_ * wt
      mapM (updateCounts buff) acc

getPMForCDF :: (PrimMonad m, KnownNat k) => ReqSketch k (PrimState m) -> [Double] -> m [Word64]
getPMForCDF this splits = do
  pure $ validateSplits splits
  let numSplits = length splits
      numBuckets = numSplits -- + 1
  splitCounts <- getCounts this splits
  n <- getN this
  pure $ (++ [n]) $ take numBuckets $ splitCounts

-- | Returns an approximation to the Cumulative Distribution Function (CDF), which is the cumulative analog of the PMF, 
-- of the input stream given a set of splitPoint (values).
cumulativeDistributionFunction 
  :: (PrimMonad m, KnownNat k) 
  => ReqSketch k (PrimState m) 
  -> [Double] 
  -- ^ Returns an approximation to the Cumulative Distribution Function (CDF), 
  -- which is the cumulative analog of the PMF, of the input stream given a set of 
  -- splitPoint (values).
  --
  -- The resulting approximations have a probabilistic guarantee that be obtained, 
  -- a priori, from the getRSE(int, double, boolean, long) function.
  --
  -- If the sketch is empty this returns 'Nothing'.
  -> m (Maybe [Double])
cumulativeDistributionFunction this splitPoints = do
  isEmpty <- getIsEmpty this
  if isEmpty
    then pure Nothing
    else do
      let numBuckets = length splitPoints + 1
      buckets <- getPMForCDF this splitPoints
      n <- getN this
      pure $ Just $ (/ fromIntegral n) . fromIntegral <$> buckets

rankAccuracy :: ReqSketch s k -> RankAccuracy
rankAccuracy = rankAccuracySetting

-- | Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]). Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were modified based on empirical measurements.
relativeStandardError
  :: ReqSketch s k
  -> Int
  -- ^ k - the given value of k
  -> Double
  -- ^ rank - the given normalized rank, a number in [0,1].
  -> RankAccuracy
  -> Int
  -- ^ totalN - an estimate of the total number of items submitted to the sketch.
  -> Double
  -- ^ an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
relativeStandardError = undefined

-- | Gets the smallest value seen by this sketch
minimum :: PrimMonad m => ReqSketch k (PrimState m) -> m Double
minimum = readURef . minValue

-- | Gets the largest value seen by this sketch
maximum :: PrimMonad m => ReqSketch k (PrimState m) -> m Double
maximum = readURef . maxValue

count :: (PrimMonad m, s ~ PrimState m, KnownNat k) => ReqSketch k s -> Double -> m Word64
count s value = fromIntegral <$> do
  empty <- null s
  if empty
    then pure 0
    else do
      compactors <- getCompactors s
      let go accum compactor = do
            let wt = (1 `shiftL` fromIntegral (Compactor.getLgWeight compactor)) :: Word64
            buf <- Compactor.getBuffer compactor
            count_ <- DoubleBuffer.getCountWithCriterion buf value (criterion s)
            pure (accum + (fromIntegral count_ * wt))
      Vector.foldM go 0 compactors

-- | Returns an approximation to the Probability Mass Function (PMF) of the input stream given a set of splitPoints (values).
-- The resulting approximations have a probabilistic guarantee that be obtained, a priori, from the getRSE(int, double, boolean, long) function.
--
-- If the sketch is empty this returns an empty list.
probabilityMassFunction
  :: ReqSketch k s
  -> [Double]
  -- ^ splitPoints - an array of m unique, monotonically increasing double values that divide 
  -- the real number line into m+1 consecutive disjoint intervals. The definition of an "interval" 
  -- is inclusive of the left splitPoint (or minimum value) and exclusive of the right splitPoint, 
  -- with the exception that the last interval will include the maximum value. It is not necessary 
  -- to include either the min or max values in these splitpoints.
  -> [Double]
  -- ^ An array of m+1 doubles each of which is an approximation to the fraction of 
  -- the input stream values (the mass) that fall into one of those intervals. 
  -- The definition of an "interval" is inclusive of the left splitPoint and exclusive 
  -- of the right splitPoint, with the exception that the last interval will 
  -- include maximum value.
probabilityMassFunction  = undefined

-- | Gets the approximate quantile of the given normalized rank based on the lteq criterion.
quantile
  :: ReqSketch k s
  -> Double
  -- ^ normRank - the given normalized rank
  -> Double
  -- ^ the approximate quantile given the normalized rank.
quantile = undefined

-- | Gets an array of quantiles that correspond to the given array of normalized ranks.
quantiles
  :: ReqSketch k s
  -> [Double]
  -- ^ normRanks - the given array of normalized ranks.
  -> Double
  -- ^ the array of quantiles that correspond to the given array of normalized ranks.
quantiles = undefined

-- | Computes the normalized rank of the given value in the stream. The normalized rank is the fraction of values less than the given value; or if lteq is true, the fraction of values less than or equal to the given value.
rank :: (PrimMonad m, s ~ PrimState m, KnownNat k)
  => ReqSketch k s
  -> Double
  -- ^ value - the given value
  -> m Double
  -- ^ the normalized rank of the given value in the stream.
rank s value = do
  isEmpty <- null s
  if isEmpty
    then pure (0 / 0) -- NaN
    else do
      nnCount <- count s value
      total <- readURef $ totalN s
      pure (fromIntegral nnCount / fromIntegral total)

-- | Returns an approximate lower bound rank of the given normalized rank.
rankLowerBound
  :: ReqSketch s k
  -> Double
  -- ^ rank - the given rank, a value between 0 and 1.0.
  -> Int
  -- ^ numStdDev - the number of standard deviations. Must be 1, 2, or 3.
  -> Double
  -- ^ an approximate lower bound rank.
rankLowerBound s rank numStdDev = undefined

-- | Gets an array of normalized ranks that correspond to the given array of values.
ranks :: (PrimMonad m, s ~ PrimState m, KnownNat k) => ReqSketch k s -> [Double] -> m [Double]
ranks s values = do
  isEmpty <- null s
  if isEmpty
    then pure []
    else do
      error "TODO"

-- | Returns an approximate upper bound rank of the given rank.
rankUpperBound
  :: ReqSketch s k
  -> Double
  -- ^ rank - the given rank, a value between 0 and 1.0.
  -> Int
  -- ^ numStdDev - the number of standard deviations. Must be 1, 2, or 3.
  -> Double
  -- ^ an approximate upper bound rank.
rankUpperBound = undefined

-- | Returns true if this sketch is empty.
null :: (PrimMonad m) => ReqSketch k (PrimState m) -> m Bool
null = fmap (== 0) . readURef . totalN

-- | Returns true if this sketch is in estimation mode.
isEstimationMode :: PrimMonad m => ReqSketch k (PrimState m) -> m Bool
isEstimationMode = fmap (> 1) . getNumLevels

-- | Returns the current comparison criterion.
isLessThanOrEqual :: ReqSketch s k -> Bool
isLessThanOrEqual s = case criterion s of
  (:<) -> False
  (:<=) -> True

computeMaxNominalSize :: PrimMonad m => ReqSketch k (PrimState m) -> m Int
computeMaxNominalSize this = do
  compactors <- getCompactors this
  Vector.foldM countNominalCapacity 0 compactors
  where
    countNominalCapacity acc compactor = do
      nominalCapacity <- Compactor.getNominalCapacity compactor
      pure $ nominalCapacity + acc

computeTotalRetainedItems :: PrimMonad m => ReqSketch k (PrimState m) -> m Int
computeTotalRetainedItems this = do
  compactors <- getCompactors this
  Vector.foldM countBuffer 0 compactors
  where
    countBuffer acc compactor = do
      buff <- Compactor.getBuffer compactor
      buffSize <- DoubleBuffer.getCount buff
      pure $ buffSize + acc

grow :: PrimMonad m => ReqSketch k (PrimState m) -> m ()
grow this = do
  lgWeight <- getNumLevels this
  compactors <- getCompactors this
  let compactors' = Vector.snoc compactors $ undefined
  maxNominalCapacity <- computeMaxNominalSize this
  writeURef (maxNominalCapacitiesSize this) maxNominalCapacity

compress :: (PrimMonad m) => ReqSketch k (PrimState m) -> m ()
compress this = do
  compactors <- getCompactors this
  for_ (Vector.indexed compactors) $ \(height, compactor) -> do
    buff <- Compactor.getBuffer compactor
    buffSize <- DoubleBuffer.getCount buff
    nominalCapacity <- Compactor.getNominalCapacity compactor
    when (buffSize >= nominalCapacity) $ do
      numLevels <- getNumLevels this
      when (height + 1 > numLevels) $ do
        grow this
      cReturn <- Compactor.compact compactor
      let topCompactor = compactors ! (height + 1)
      buff <- Compactor.getBuffer topCompactor
      DoubleBuffer.mergeSortIn buff $ Compactor.crDoubleBuffer cReturn
      modifyURef (retainedItems this) (+ Compactor.crDeltaRetItems cReturn)
      modifyURef (maxNominalCapacitiesSize this) (+ Compactor.crDeltaNominalSize cReturn)
  writeMutVar (aux this) Nothing

-- | Merge other sketch into this one.
merge
  :: (PrimMonad m, s ~ PrimState m)
  => ReqSketch k s
  -> ReqSketch k s
  -> m (ReqSketch k s)
merge this other = do
  otherIsEmpty <- getIsEmpty other
  unless otherIsEmpty $ do
    let rankAccuracy = rankAccuracySetting this
        otherRankAccuracy = rankAccuracySetting other
    when (rankAccuracy /= otherRankAccuracy) $
      error "Both sketches must have the same HighRankAccuracy setting."
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
    numRequiredCompactors <- getNumLevels other
    growUntil numRequiredCompactors
    -- merge the items in all height compactors
    thisCompactors <- getCompactors this
    otherCompactors <- getCompactors other
    Vector.zipWithM_ Compactor.merge thisCompactors otherCompactors
    -- update state
    maxNominalCapacity <- computeMaxNominalSize this
    totalRetainedItems <- computeTotalRetainedItems this
    writeURef (maxNominalCapacitiesSize this) maxNominalCapacity
    writeURef (retainedItems this) totalRetainedItems
    -- compress and check invariants
    when (totalRetainedItems >= maxNominalCapacity) $ do
      compress this
    unless (totalRetainedItems < maxNominalCapacity) $ do
      error "invariant violated: totalRetainedItems is not smaller than maxNominalCapacity"
    writeMutVar (aux this) Nothing
  pure this
  where
    growUntil target = do
      numCompactors <- getNumLevels this
      when (numCompactors < target) $
        grow this

-- | Updates this sketch with the given item.
update :: (PrimMonad m) => ReqSketch k (PrimState m) -> Double -> m ()
update this item = do
  unless (isNaN item) $ do
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
    compactor <- (! 0) <$> getCompactors this
    buff <- Compactor.getBuffer compactor
    modifyURef (retainedItems this) (+1)
    modifyURef (totalN this) (+1)
    retItems <-  getRetainedItems this
    maxNominalCapacity <- getMaxNominalCapacity this
    when (retItems < maxNominalCapacity) $ do
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
exactRank k levels rank hra totalN = (levels == 1 || fromIntegral totalN <= baseCap) || (hra && rank >= 1.0 - exactRankThresh || not hra && rank <= exactRankThresh)
  where
    baseCap = k * initNumberOfSections
    exactRankThresh :: Double
    exactRankThresh = fromIntegral baseCap / fromIntegral totalN

getRSE :: Int -> Double -> Bool -> Word64 -> Double
getRSE k rank hra totalN = getRankUB k 2 rank 1 hra totalN
