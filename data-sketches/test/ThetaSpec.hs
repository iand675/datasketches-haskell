{-# LANGUAGE TypeApplications #-}
module ThetaSpec where

import Control.Monad (forM_)
import Data.Word
import Test.Hspec
import qualified DataSketches.Distinct.Theta as Theta

spec :: Spec
spec = describe "Theta Sketch" $ do
  specify "empty sketch estimates 0" $ do
    sk <- Theta.mkThetaSketch 128
    est <- Theta.estimate sk
    est `shouldBe` 0.0
    Theta.isEmpty sk `shouldReturn` True

  specify "single item estimates 1" $ do
    sk <- Theta.mkThetaSketch 128
    Theta.insert sk 42
    est <- Theta.estimate sk
    est `shouldBe` 1.0
    Theta.isEmpty sk `shouldReturn` False

  specify "exact mode for small cardinality" $ do
    sk <- Theta.mkThetaSketch 4096
    forM_ [1..100 :: Word64] $ Theta.insert sk
    est <- Theta.estimate sk
    est `shouldBe` 100.0

  specify "duplicate items don't increase estimate" $ do
    sk <- Theta.mkThetaSketch 4096
    forM_ [1..50 :: Word64] $ Theta.insert sk
    est1 <- Theta.estimate sk
    forM_ [1..50 :: Word64] $ Theta.insert sk
    est2 <- Theta.estimate sk
    est2 `shouldBe` est1

  specify "1000 distinct items estimates approximately 1000" $ do
    sk <- Theta.mkThetaSketch 4096
    forM_ [1..1000 :: Word64] $ Theta.insert sk
    est <- Theta.estimate sk
    est `shouldSatisfy` (\e -> abs (e - 1000) < 100)

  specify "10000 items with smaller k estimates approximately" $ do
    sk <- Theta.mkThetaSketch 256
    forM_ [1..10000 :: Word64] $ Theta.insert sk
    est <- Theta.estimate sk
    est `shouldSatisfy` (\e -> abs (e - 10000) < 2000)

  describe "union" $ do
    specify "union of disjoint sets" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..500 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      forM_ [501..1000 :: Word64] $ Theta.insert sk2
      u <- Theta.union sk1 sk2
      est <- Theta.estimate u
      est `shouldSatisfy` (\e -> abs (e - 1000) < 100)

    specify "union with empty sketch" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..100 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      u <- Theta.union sk1 sk2
      est <- Theta.estimate u
      est `shouldBe` 100.0

  describe "intersection" $ do
    specify "intersection of overlapping sets" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..200 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      forM_ [101..300 :: Word64] $ Theta.insert sk2
      i <- Theta.intersection sk1 sk2
      est <- Theta.estimate i
      -- 100 items overlap (101..200)
      est `shouldSatisfy` (\e -> abs (e - 100) < 30)

    specify "intersection of disjoint sets is ~0" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..100 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      forM_ [101..200 :: Word64] $ Theta.insert sk2
      i <- Theta.intersection sk1 sk2
      est <- Theta.estimate i
      est `shouldBe` 0.0

    specify "intersection with empty sketch is 0" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..100 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      i <- Theta.intersection sk1 sk2
      est <- Theta.estimate i
      est `shouldBe` 0.0

  describe "difference" $ do
    specify "difference removes overlapping items" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..200 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      forM_ [101..300 :: Word64] $ Theta.insert sk2
      d <- Theta.difference sk1 sk2
      est <- Theta.estimate d
      -- sk1 has 1..200, sk2 has 101..300. A \ B = 1..100
      est `shouldSatisfy` (\e -> abs (e - 100) < 30)

    specify "difference with empty B returns A" $ do
      sk1 <- Theta.mkThetaSketch 4096
      forM_ [1..100 :: Word64] $ Theta.insert sk1
      sk2 <- Theta.mkThetaSketch 4096
      d <- Theta.difference sk1 sk2
      est <- Theta.estimate d
      est `shouldBe` 100.0
