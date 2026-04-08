{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeApplications #-}
-- | Relative Error Quantiles (REQ) sketch for approximate quantile computation
-- with /relative/ error bounds.
--
-- Most quantile sketches (like KLL) have /additive/ error: the answer might
-- be off by, say, 1.3% of the full rank range no matter where you query.
-- That's fine for p50 or p90, but at p99.99 a 1.3% additive error is enormous
-- relative to the sliver of the distribution you're looking at.
--
-- REQ flips this: the error is /proportional to the distance from the nearest
-- tail/. At p99.99, the error is proportional to 0.01%, not 1.3%. This makes
-- REQ the right choice when you care specifically about extreme percentiles —
-- SLA monitoring at p99.9, tail-latency debugging, outlier detection.
--
-- For general-purpose quantiles where you don't need tail precision, prefer
-- "DataSketches.Quantiles.KLL" — it's faster and simpler.
--
-- Based on "Relative Error Streaming Quantiles" (Cormode et al., PODS 2021).
--
-- === Configuration
--
-- @k@ controls accuracy vs. space. Must be even, in @[4, 1024]@. Larger @k@
-- means tighter error bounds but more memory. @k = 12@ is a reasonable default.
--
-- 'RankAccuracy' selects which tail gets the better error guarantee:
--
-- * 'HighRanksAreAccurate' — use when you care about p99, p99.9, p99.99
-- * 'LowRanksAreAccurate' — use when you care about p1, p0.1, p0.01
--
-- === Usage
--
-- @
-- import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
--
-- main :: IO ()
-- main = do
--   sk <- REQ.'mkReqSketch' 12 REQ.'HighRanksAreAccurate'
--   mapM_ (REQ.'insert' sk) [1..100000 :: Double]
--   p999 <- REQ.'quantile' sk 0.999
--   putStrLn $ "p99.9 = " ++ show p999
-- @
--
-- === Implementation
--
-- Backed by a C implementation (@cbits\/req.c@) behind a 'ForeignPtr'.
-- All sketch memory lives outside the GHC heap — zero GC pressure,
-- zero boxing overhead.
--
-- === Mergeability
--
-- REQ sketches are fully mergeable: two sketches built on separate data
-- partitions can be combined via 'merge' to produce a sketch equivalent
-- to having processed all data in a single stream. Both sketches must
-- share the same 'RankAccuracy' setting.
module DataSketches.Quantiles.RelativeErrorQuantile (
  -- * Construction
    ReqSketch
  , mkReqSketch
  -- ** Configuration settings
  , RankAccuracy(..)
  -- * Querying
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
  , numLevels
  -- * Updating the sketch
  , merge
  , insert
  , insertBatch
  -- * Inspection
  , rankAccuracy
  , isEstimationMode
  , isLessThanOrEqual
  , setCriterionLE
  , setCriterionLT
  , DoubleIsNonFiniteException(..)
  ) where

import Control.Exception (throw)
import Control.Monad (when)
import Control.Monad.Primitive ( PrimMonad(PrimState) )
import Data.Word ( Word32, Word64 )
import DataSketches.Quantiles.RelativeErrorQuantile.Types
    ( RankAccuracy(..), DoubleIsNonFiniteException(..) )
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants
    ( fixRseFactor, initNumberOfSections, relRseFactor )
import DataSketches.Quantiles.RelativeErrorQuantile.CInternal
import qualified Data.List
import qualified Data.Vector.Storable as VS
import Prelude hiding (sum, minimum, maximum, null)

type ReqSketch = CReqSketch

mkReqSketch :: PrimMonad m
  => Word32
  -> RankAccuracy
  -> m (ReqSketch (PrimState m))
mkReqSketch k ra = mkCReqSketch k (raToInt ra)
  where
    raToInt HighRanksAreAccurate = 1
    raToInt LowRanksAreAccurate  = 0

insert :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m ()
insert = creqInsert
{-# INLINE insert #-}

-- | Bulk-insert a storable vector of doubles. Avoids per-element FFI overhead.
insertBatch :: PrimMonad m => ReqSketch (PrimState m) -> VS.Vector Double -> m ()
insertBatch = creqInsertBatch

merge :: PrimMonad m => ReqSketch (PrimState m) -> ReqSketch (PrimState m) -> m (ReqSketch (PrimState m))
merge dst src = creqMerge dst src >> pure dst

count :: PrimMonad m => ReqSketch (PrimState m) -> m Word64
count = creqCount
{-# INLINE count #-}

null :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
null = creqIsEmpty

minimum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
minimum = creqMin

maximum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
maximum = creqMax

sum :: PrimMonad m => ReqSketch (PrimState m) -> m Double
sum = creqSum

retainedItemCount :: PrimMonad m => ReqSketch (PrimState m) -> m Int
retainedItemCount = creqRetained

getK :: PrimMonad m => ReqSketch (PrimState m) -> m Word32
getK = creqK

rankAccuracy :: PrimMonad m => ReqSketch (PrimState m) -> m RqAcc
rankAccuracy sk = do
  i <- creqRankAccuracy sk
  pure $ if i == 1 then HighRanksAreAccurate else LowRanksAreAccurate

type RqAcc = RankAccuracy

numLevels :: PrimMonad m => ReqSketch (PrimState m) -> m Int
numLevels = creqNumLevels

isEstimationMode :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
isEstimationMode sk = (> 1) <$> creqNumLevels sk

isLessThanOrEqual :: PrimMonad m => ReqSketch (PrimState m) -> m Bool
isLessThanOrEqual sk = (== 1) <$> creqCriterion sk

setCriterionLE :: PrimMonad m => ReqSketch (PrimState m) -> m ()
setCriterionLE sk = creqSetCriterion sk 1

setCriterionLT :: PrimMonad m => ReqSketch (PrimState m) -> m ()
setCriterionLT sk = creqSetCriterion sk 0

countWithCriterion :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m Word64
countWithCriterion sk value = do
  when (isNaN value || isInfinite value) $ throw (DoubleIsNonFiniteException value)
  creqCountWithCriterion sk value

rank :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m Double
rank sk value = do
  when (isNaN value || isInfinite value) $ throw (DoubleIsNonFiniteException value)
  creqRank sk value

ranks :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Double]
ranks sk = mapM (rank sk)

quantile :: PrimMonad m => ReqSketch (PrimState m) -> Double -> m Double
quantile sk normRank = do
  empty <- null sk
  if empty
    then pure (0/0)
    else do
      when (normRank < 0 || normRank > 1.0) $
        error $ "Normalized rank must be in the range [0.0, 1.0]: " ++ show normRank
      creqQuantile sk normRank

quantiles :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Double]
quantiles sk = mapM (quantile sk)

rankLowerBound :: PrimMonad m => ReqSketch (PrimState m) -> Double -> Int -> m Double
rankLowerBound sk r numStdDev = do
  numLevels <- creqNumLevels sk
  k_ <- fromIntegral <$> getK sk
  total <- count sk
  ra <- rankAccuracy sk
  pure $ getRankLB k_ numLevels r numStdDev (ra == HighRanksAreAccurate) total

rankUpperBound :: PrimMonad m => ReqSketch (PrimState m) -> Double -> Int -> m Double
rankUpperBound sk r numStdDev = do
  numLevels <- creqNumLevels sk
  k_ <- fromIntegral <$> getK sk
  total <- count sk
  ra <- rankAccuracy sk
  pure $ getRankUB k_ numLevels r numStdDev (ra == HighRanksAreAccurate) total

relativeStandardError :: Int -> Double -> RankAccuracy -> Word64 -> Double
relativeStandardError k_ rank_ hra = getRankUB k_ 2 rank_ 1 (hra == HighRanksAreAccurate)

cumulativeDistributionFunction
  :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m (Maybe [Double])
cumulativeDistributionFunction sk splitPoints = do
  validateSplits splitPoints
  empty <- null sk
  if empty
    then pure Nothing
    else do
      rs <- mapM (rank sk) splitPoints
      pure $ Just (rs ++ [1.0])

probabilityMassFunction :: PrimMonad m => ReqSketch (PrimState m) -> [Double] -> m [Double]
probabilityMassFunction sk splitPoints = do
  empty <- null sk
  if empty
    then pure []
    else do
      rs <- mapM (rank sk) splitPoints
      let cdf = rs ++ [1.0]
          pmf = zipWith (-) cdf (0.0 : cdf)
      pure pmf

-- Private helpers

validateSplits :: Monad m => [Double] -> m ()
validateSplits splits = do
  when (Data.List.null splits) $
    error "splits must not be empty"
  case filter (\x -> isInfinite x || isNaN x) splits of
    (badValue:_) -> throw (DoubleIsNonFiniteException badValue)
    [] -> pure ()

getRankLB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankLB k_ levels r numStdDev hra totalN_ =
  if exactRank k_ levels r hra totalN_ then r
  else max lbRel lbFix
  where
    relative = relRseFactor / fromIntegral k_ * (if hra then 1.0 - r else r)
    fixed = fixRseFactor / fromIntegral k_
    lbRel = r - fromIntegral numStdDev * relative
    lbFix = r - fromIntegral numStdDev * fixed

getRankUB :: Int -> Int -> Double -> Int -> Bool -> Word64 -> Double
getRankUB k_ levels r numStdDev hra totalN_ =
  if exactRank k_ levels r hra totalN_ then r
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
