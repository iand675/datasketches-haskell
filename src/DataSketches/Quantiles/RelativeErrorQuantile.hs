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

import Data.Word
import DataSketches.Quantilles.RelativeErrorQuantile.Types
import DataSketches.Quantilles.RelativeErrorQuantile.Compactor

data ReqSketch = ReqSketch
  { sectionSize :: !Int -- ^ Referred to as k in the paper
  , rankAccuracySetting :: !RankAccuracy
  , ltEq :: Bool
  , totalN :: !Word64
  , minValue :: !Double
  , maxValue :: !Double 
  , retainedItems :: !Int
  , maxNominalCapacitiesSize :: !Int
  , aux :: ()
  , compactors :: [ReqCompactor]
  }

cumulativeDistributionFunction :: [Double] -> ReqSketch -> [Double]
cumulativeDistributionFunction splitPoints = undefined


rankAccuracy :: ReqSketch -> RankAccuracy
rankAccuracy = undefined

relativeStandardError :: ReqSketch -> Int -> Double -> RankAccuracy -> Int -> Double
relativeStandardError = undefined 

minimum :: ReqSketch -> Double
minimum = _

maximum :: ReqSketch -> Double
maximum = _

count :: ReqSketch -> Word64
count = _

probabilityMassFunction :: ReqSketch -> [Double] -> [Double]
probabilityMassFunction  = undefined

quantile :: ReqSketch -> Double -> Double
quantile = _

quantiles :: ReqSketch -> [Double] -> Double
quantiles = _

rank :: ReqSketch -> Double -> Double 
rank = _

rankLowerBound :: ReqSketch -> Double -> Int -> Double
rankLowerBound = undefined

ranks :: ReqSketch -> [Double] -> [Double]
ranks = undefined

rankUpperBound :: ReqSketch -> Double -> Int -> Double
rankUpperBound = undefined

retainedItems :: ReqSketch -> Int
retainedItems = undefined

-- Foldable

null :: ReqSketch -> Bool
null = undefined

isEstimationMode :: ReqSketch -> Bool
isEstimationMode = undefined

isLessThanOrEqual :: ReqSketch -> Bool
isLessThanOrEqual = undefined

merge :: ReqSketch -> ReqSketch -> ReqSketch
merge = undefined

-- TODO reset?

update :: ReqSketch -> Double -> ReqSketch
update = undefined
