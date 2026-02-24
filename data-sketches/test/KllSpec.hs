{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE LambdaCase #-}
module KllSpec where

import Control.Monad (forM_)
import Control.Monad.Primitive (PrimState)
import Data.Word
import Test.Hspec
import Test.QuickCheck
import qualified DataSketches.Quantiles.KLL as KLL

spec :: Spec
spec = describe "KLL Sketch" $ do
  specify "empty sketch returns NaN for quantile" $ do
    sk <- KLL.mkKllSketch 200
    q <- KLL.quantile sk 0.5
    isNaN q `shouldBe` True

  specify "empty sketch returns True for null" $ do
    sk <- KLL.mkKllSketch 200
    KLL.null sk `shouldReturn` True

  specify "single element sketch returns the element for all quantiles" $ do
    sk <- KLL.mkKllSketch 200
    KLL.insert sk 42.0
    KLL.null sk `shouldReturn` False
    KLL.count sk `shouldReturn` 1
    KLL.minimum sk `shouldReturn` 42.0
    KLL.maximum sk `shouldReturn` 42.0
    q <- KLL.quantile sk 0.5
    q `shouldBe` 42.0

  specify "tracks min and max correctly" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [10, 5, 20, 1, 100 :: Double] $ KLL.insert sk
    KLL.minimum sk `shouldReturn` 1.0
    KLL.maximum sk `shouldReturn` 100.0
    KLL.count sk `shouldReturn` 5

  specify "ignores NaN values" $ do
    sk <- KLL.mkKllSketch 200
    KLL.insert sk (0/0)
    KLL.null sk `shouldReturn` True

  specify "sequential insert produces correct count" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..1000 :: Double] $ KLL.insert sk
    KLL.count sk `shouldReturn` 1000

  specify "quantile 0 returns minimum" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..100 :: Double] $ KLL.insert sk
    q <- KLL.quantile sk 0.0
    q `shouldBe` 1.0

  specify "quantile 1 returns maximum" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..100 :: Double] $ KLL.insert sk
    q <- KLL.quantile sk 1.0
    q `shouldBe` 100.0

  specify "rank is monotonically non-decreasing" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..200 :: Double] $ KLL.insert sk
    rs <- KLL.ranks sk [10, 50, 100, 150, 190]
    rs `shouldSatisfy` isNonDecreasing

  specify "median of sequential data is approximately correct" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..1000 :: Double] $ KLL.insert sk
    median <- KLL.quantile sk 0.5
    -- With k=200, error ~1.3%, so median should be within ~13 of 500
    median `shouldSatisfy` (\m -> abs (m - 500) < 50)

  specify "merge combines two sketches correctly" $ do
    sk1 <- KLL.mkKllSketch 200
    forM_ [1..100 :: Double] $ KLL.insert sk1
    sk2 <- KLL.mkKllSketch 200
    forM_ [101..200 :: Double] $ KLL.insert sk2
    KLL.merge sk1 sk2
    KLL.count sk1 `shouldReturn` 200
    KLL.minimum sk1 `shouldReturn` 1.0
    KLL.maximum sk1 `shouldReturn` 200.0

  specify "merge with empty sketch is identity" $ do
    sk1 <- KLL.mkKllSketch 200
    forM_ [1..50 :: Double] $ KLL.insert sk1
    sk2 <- KLL.mkKllSketch 200
    KLL.merge sk1 sk2
    KLL.count sk1 `shouldReturn` 50

  specify "large insert triggers compaction" $ do
    sk <- KLL.mkKllSketch 8
    forM_ [1..10000 :: Double] $ KLL.insert sk
    retained <- KLL.retainedItemCount sk
    retained `shouldSatisfy` (< 10000)
    KLL.count sk `shouldReturn` 10000

  specify "CDF returns Just for non-empty sketch" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..100 :: Double] $ KLL.insert sk
    cdf <- KLL.cumulativeDistributionFunction sk [25, 50, 75]
    cdf `shouldSatisfy` \case
      Just xs -> length xs == 4 && all (\x -> x >= 0 && x <= 1) xs
      Nothing -> False

  specify "PMF sums approximately to 1" $ do
    sk <- KLL.mkKllSketch 200
    forM_ [1..100 :: Double] $ KLL.insert sk
    pmf <- KLL.probabilityMassFunction sk [25, 50, 75]
    length pmf `shouldBe` 4
    sum pmf `shouldSatisfy` (\s -> abs (s - 1.0) < 0.01)

isNonDecreasing :: (Ord a) => [a] -> Bool
isNonDecreasing [] = True
isNonDecreasing [_] = True
isNonDecreasing (x:y:rest) = x <= y && isNonDecreasing (y:rest)
