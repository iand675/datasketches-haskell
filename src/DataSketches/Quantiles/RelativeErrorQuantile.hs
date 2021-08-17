{-# LANGUAGE GADTs #-}
module DataSketches.Quantiles.RelativeErrorQuantile
  ( cumulativeDistributionFunction  
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

import Control.Monad.Primitive
import Data.Bits (shiftL)
import qualified Data.Vector.Mutable as MVector
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
  , aux :: ()
  , compactors :: MVector.MVector s (ReqCompactor s k)
  }

getNumLevels :: ReqSketch s k -> Int
getNumLevels = MVector.length . compactors

cumulativeDistributionFunction :: PrimMonad m => [Double] -> ReqSketch (PrimState m) k -> m [Double]
cumulativeDistributionFunction splitPoints = undefined

rankAccuracy :: ReqSketch s k -> RankAccuracy
rankAccuracy = undefined

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

null :: (PrimMonad m) => ReqSketch (PrimState m) k -> m Bool
null = fmap (== 0) . readURef . totalN

isEstimationMode :: ReqSketch s k -> Bool
isEstimationMode = (> 1) . getNumLevels

isLessThanOrEqual :: ReqSketch s k -> Bool
isLessThanOrEqual s = case criterion s of
  (:<) -> False
  (:<=) -> True

merge 
  :: (PrimMonad m, s ~ PrimState m)
  => ReqSketch s k 
  -> ReqSketch s k 
  -> m (ReqSketch s k)
merge this other = undefined

-- TODO reset?

update :: PrimMonad m => ReqSketch (PrimState m) k -> Double -> m ()
update = undefined

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
