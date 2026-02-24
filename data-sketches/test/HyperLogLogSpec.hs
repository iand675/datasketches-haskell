{-# LANGUAGE TypeApplications #-}
module HyperLogLogSpec where

import Control.Monad (forM_)
import Data.Word
import Test.Hspec
import qualified DataSketches.Distinct.HyperLogLog as HLL

spec :: Spec
spec = describe "HyperLogLog Sketch" $ do
  specify "empty sketch estimates 0" $ do
    sk <- HLL.mkHllSketch 12
    est <- HLL.estimate sk
    est `shouldBe` 0.0

  specify "single item estimates ~1" $ do
    sk <- HLL.mkHllSketch 12
    HLL.insert sk 42
    est <- HLL.estimate sk
    est `shouldSatisfy` (\e -> e >= 0.5 && e <= 2.0)

  specify "100 distinct items estimates approximately 100" $ do
    sk <- HLL.mkHllSketch 12
    forM_ [1..100 :: Word64] $ HLL.insert sk
    est <- HLL.estimate sk
    -- With p=12, error ~1.6%, so within ~20 is very generous
    est `shouldSatisfy` (\e -> abs (e - 100) < 30)

  specify "1000 distinct items estimates approximately 1000" $ do
    sk <- HLL.mkHllSketch 14
    forM_ [1..1000 :: Word64] $ HLL.insert sk
    est <- HLL.estimate sk
    est `shouldSatisfy` (\e -> abs (e - 1000) < 100)

  specify "10000 distinct items estimates approximately 10000" $ do
    sk <- HLL.mkHllSketch 14
    forM_ [1..10000 :: Word64] $ HLL.insert sk
    est <- HLL.estimate sk
    est `shouldSatisfy` (\e -> abs (e - 10000) < 1000)

  specify "duplicate items don't increase estimate" $ do
    sk <- HLL.mkHllSketch 12
    forM_ [1..100 :: Word64] $ HLL.insert sk
    est1 <- HLL.estimate sk
    -- Insert all the same items again
    forM_ [1..100 :: Word64] $ HLL.insert sk
    est2 <- HLL.estimate sk
    -- Estimate should be about the same (still ~100)
    abs (est2 - est1) `shouldSatisfy` (< 5)

  specify "merge combines two sketches" $ do
    sk1 <- HLL.mkHllSketch 12
    forM_ [1..500 :: Word64] $ HLL.insert sk1
    sk2 <- HLL.mkHllSketch 12
    forM_ [501..1000 :: Word64] $ HLL.insert sk2
    HLL.merge sk1 sk2
    est <- HLL.estimate sk1
    est `shouldSatisfy` (\e -> abs (e - 1000) < 100)

  specify "merge with overlapping items gives correct estimate" $ do
    sk1 <- HLL.mkHllSketch 12
    forM_ [1..500 :: Word64] $ HLL.insert sk1
    sk2 <- HLL.mkHllSketch 12
    forM_ [250..750 :: Word64] $ HLL.insert sk2
    HLL.merge sk1 sk2
    est <- HLL.estimate sk1
    -- 750 distinct items
    est `shouldSatisfy` (\e -> abs (e - 750) < 100)

  specify "precision is reported correctly" $ do
    sk <- HLL.mkHllSketch 14
    p <- HLL.precision sk
    p `shouldBe` 14
