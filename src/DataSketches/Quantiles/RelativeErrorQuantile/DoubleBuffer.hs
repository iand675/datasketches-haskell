module DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer 
  ( DoubleBuffer
  , append
  , ensureCapacity
  , getCountWithCriterion
  , getEvensOrOdds
  , (!) -- getItem
  , getDelta
  , getCount
  , getSpace
  , isSpaceAtBottom
  , isEmpty
  , isSorted
  , sort
  , mergeSortIn
  , trimCapacity
  , trimCount
  ) where

import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Control.Monad.Primitive
import qualified Data.Vector.Unboxed as MUVector
import Data.Vector.Unboxed

data DoubleBuffer = DoubleBuffer
  {
  }

append :: DoubleBuffer -> Double -> m ()
append = undefined 

ensureCapacity :: DoubleBuffer -> Int -> m ()
ensureCapacity = undefined

getCountWithCriterion :: DoubleBuffer -> Double -> Criterion -> m Int
getCountWithCriterion = undefined 

data EvensOrOdds = Evens | Odds

getEvensOrOdds :: DoubleBuffer -> Int -> Int -> EvensOrOdds -> m DoubleBuffer
getEvensOrOdds = undefined

(!) :: DoubleBuffer -> Int -> m Double
buf ! i = undefined

