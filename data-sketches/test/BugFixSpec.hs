{-# LANGUAGE TypeApplications #-}
module BugFixSpec where

import Control.Monad (forM_)
import Test.Hspec
import DataSketches.Quantiles.RelativeErrorQuantile hiding (null, minimum, maximum)
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ

spec :: Spec
spec = do
  describe "Bug fix: rank boundary conditions with LowRanksAreAccurate" $ do
    specify "rank of value above max is correct" $ do
      sk <- mkReqSketch 12 LowRanksAreAccurate
      forM_ [1..50 :: Double] $ insert sk
      r <- rank sk 100
      r `shouldBe` 1.0

    specify "rank of value below min is 0" $ do
      sk <- mkReqSketch 12 LowRanksAreAccurate
      forM_ [10..60 :: Double] $ insert sk
      r <- rank sk 5
      r `shouldBe` 0.0

    specify "ranks are correct for known values" $ do
      sk <- mkReqSketch 50 LowRanksAreAccurate
      let values = [5, 5, 5, 6, 6, 6, 7, 8, 8, 8 :: Double]
      mapM_ (insert sk) values
      r5 <- rank sk 5
      r5 `shouldBe` 0.0
      r6 <- rank sk 6
      r6 `shouldBe` 0.3
      r7 <- rank sk 7
      r7 `shouldBe` 0.6
      r8 <- rank sk 8
      r8 `shouldBe` 0.7
      r9 <- rank sk 9
      r9 `shouldBe` 1.0

  describe "Bug fix: growUntil in merge didn't loop" $ do
    specify "merge into empty sketch preserves count from multi-level source" $ do
      skBig <- mkReqSketch 6 HighRanksAreAccurate
      forM_ [1..2000 :: Double] $ insert skBig
      bigLevels <- numLevels skBig
      bigLevels `shouldSatisfy` (>= 3)

      skSmall <- mkReqSketch 6 HighRanksAreAccurate
      _ <- merge skSmall skBig

      mergedCount <- count skSmall
      mergedCount `shouldBe` 2000

      smallLevels <- numLevels skSmall
      smallLevels `shouldSatisfy` (>= bigLevels)

    specify "merge sketch with 4+ levels into 1-level sketch" $ do
      skSrc <- mkReqSketch 6 HighRanksAreAccurate
      forM_ [1..5000 :: Double] $ insert skSrc
      srcLevels <- numLevels skSrc
      srcLevels `shouldSatisfy` (>= 4)

      skDst <- mkReqSketch 6 HighRanksAreAccurate
      insert skDst 0
      dstLevelsBefore <- numLevels skDst
      dstLevelsBefore `shouldBe` 1

      _ <- merge skDst skSrc
      dstLevelsAfter <- numLevels skDst
      dstLevelsAfter `shouldSatisfy` (>= srcLevels)
      mergedCount <- count skDst
      mergedCount `shouldBe` 5001

  describe "Bug fix: max value comparison inverted in merge" $ do
    specify "merge updates maximum when other has larger max" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [1..10 :: Double] $ insert sk1
      max1 <- REQ.maximum sk1
      max1 `shouldBe` 10.0

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [100..200 :: Double] $ insert sk2

      _ <- merge sk1 sk2
      mergedMax <- REQ.maximum sk1
      mergedMax `shouldBe` 200.0

    specify "merge does NOT update maximum when other has smaller max" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [100..200 :: Double] $ insert sk1

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [1..10 :: Double] $ insert sk2

      _ <- merge sk1 sk2
      mergedMax <- REQ.maximum sk1
      mergedMax `shouldBe` 200.0

    specify "merge with bug would have produced wrong max (both directions)" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      mapM_ (insert sk1) [1, 2, 3 :: Double]

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      mapM_ (insert sk2) [10, 20, 30 :: Double]

      _ <- merge sk1 sk2
      mergedMax <- REQ.maximum sk1
      mergedMax `shouldBe` 30.0

      mergedMin <- REQ.minimum sk1
      mergedMin `shouldBe` 1.0
