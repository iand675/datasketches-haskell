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
  --      k     min max hra                  lteq  low-to-high or high-to-low
  bigTest Proxy 1   200 HighRanksAreAccurate (:<=) True
  bigTest Proxy 1   200 LowRanksAreAccurate  (:<=) True
  bigTest Proxy 1   200 HighRanksAreAccurate (:<)  False
  bigTest Proxy 1   200 LowRanksAreAccurate  (:<)  True

  mergeSpec

bigTest :: Proxy 6 -> Int -> Int -> RankAccuracy -> Criterion -> Bool -> Spec
bigTest k min_ max_ hra crit up = do
  let testName = intercalate " "
        [ "k=" <> show (natVal k)
        , "min=" <> show min_
        , "max=" <> show max_
        , "hra=" <> show hra
        ]
      testContents :: IO ()
      testContents = do
        sk <- loadSketch k min_ max_ hra crit up
        -- checkAux sk
        checkGetRank sk min_ max_
        -- checkGetRanks sk, max
        -- checkGetQuantiles sk
        -- checkGetCDF sk
        -- checkGetPMF sk
        -- checkIterator sk
        -- checkMerge sk
  it testName testContents

asIO :: IO a -> IO a
asIO = id

mergeSpec :: Spec
mergeSpec = specify "merge works" $ asIO $ do
  s1 <- mkReqSketch HighRanksAreAccurate :: IO (ReqSketch 12 (PrimState IO))
  mapM_ (update s1) [0..40]
  s2 <- mkReqSketch HighRanksAreAccurate :: IO (ReqSketch 12 (PrimState IO))
  mapM_ (update s2) [0..40]
  s3 <- mkReqSketch HighRanksAreAccurate :: IO (ReqSketch 12 (PrimState IO))
  mapM_ (update s3) [0..40]
  s <- mkReqSketch HighRanksAreAccurate :: IO (ReqSketch 12 (PrimState IO))
  s `merge` s1
  s `merge` s2
  s `merge` s3
  pure ()

loadSketch :: forall n. (KnownNat n, ValidK n) => Proxy n -> Int -> Int -> RankAccuracy -> Criterion -> Bool -> IO (ReqSketch n (PrimState IO))
loadSketch k min_ max_ hra ltEq up = do
  sk <- mkReqSketch hra :: IO (ReqSketch n (PrimState IO))
  -- This just seems geared at making sure that ranks come out right regardless of order
  mapM_ (update sk . fromIntegral) $ if up
    then [min_ .. max_]
    else reverse [min_ .. max_ {- + 1 -}]
  pure sk

checkGetRank :: ReqSketch n (PrimState IO) -> Int -> Int -> IO ()
checkGetRank = undefined