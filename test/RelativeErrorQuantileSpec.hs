module RelativeErrorQuantileSpec where

import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Data.List
import Data.Proxy
import GHC.TypeLits
import Test.Hspec

spec :: Spec
spec = do
  pure ()

bigTest :: KnownNat n => Proxy n -> Int -> Int -> RankAccuracy -> Bool -> Spec
bigTest k min_ max_ up hra ltEq = do
  it $ intercalate " " $ intercalate "="
    [ "k", show $ natVal k
    , "min", show min_
    , "max", max_
    , "up", show up
    , "hra", show hra
    ]
  sk <- loadSketch k min max up hra ltEq
  -- checkAux sk
  checkGetRank sk min_ max_
  -- checkGetRanks sk, max
  -- checkGetQuantiles sk
  -- checkGetCDF sk
  -- checkGetPMF sk
  -- checkIterator sk
  -- checkMerge sk

loadSketch :: forall n. (KnownNat n, ValidK n) => Proxy n -> Int -> Int -> RankAccuracy -> Bool -> Spec
loadSketch k min_ max_ up hra ltEq = do
  sk <- mkReqSketch hra :: IO (ReqSketch (PrimState IO) n)
  mapM_ (update sk . fromIntegral) $ if up
    then [min_ .. max_]
    else reverse [min_ .. max_ {- + 1 -}]
  pure sk

checkGetRank = undefined