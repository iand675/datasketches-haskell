{-# LANGUAGE TypeApplications #-}
module BugFixSpec where

import Control.Monad (forM_, replicateM_)
import Control.Monad.Primitive (PrimState)
import Data.Word
import qualified Data.Vector.Unboxed.Mutable as MUVector
import Test.Hspec
import DataSketches.Quantiles.RelativeErrorQuantile hiding (null, minimum, maximum)
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import DataSketches.Quantiles.RelativeErrorQuantile.Internal
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer

spec :: Spec
spec = do
  describe "Bug fix: off-by-one in getCountWithCriterion (spaceAtBottom=False)" $ do
    -- The bug: when spaceAtBottom=False, high was set to count_ instead of
    -- count_-1, causing the binary search to read vec[count_] which is past
    -- the active region. This read garbage/uninitialized memory, producing
    -- intermittent incorrect rank calculations.

    specify "getCountWithCriterion reads only within active region (spaceAtBottom=False)" $ do
      -- Create a buffer with extra capacity and spaceAtBottom=False.
      -- Write poison values in the unused region to make the bug deterministic.
      buf <- mkBuffer 16 0 False  -- capacity=16, spaceAtBottom=False
      mapM_ (append buf) [10, 20, 30, 40, 50]
      sort buf

      -- Poison the memory just past the active region (index 5..15).
      -- Before the fix, the search would read index 5 (the first poison value).
      vec <- getVector buf
      forM_ [5..15] $ \i -> MUVector.unsafeWrite vec i 0

      -- With the old bug: searching for 55 (> all elements) would read
      -- vec[5]=0, causing the binary search to return a wrong answer.
      -- The fix ensures the search only reads indices 0..4.
      cnt <- getCountWithCriterion buf 55 (:<)
      cnt `shouldBe` 5

    specify "getCountWithCriterion at upper boundary (spaceAtBottom=False)" $ do
      buf <- mkBuffer 16 0 False
      mapM_ (append buf) [10, 20, 30, 40, 50]
      sort buf

      vec <- getVector buf
      -- Poison with a value that would confuse the search
      forM_ [5..15] $ \i -> MUVector.unsafeWrite vec i 25

      -- Value equal to max element
      cnt <- getCountWithCriterion buf 50 (:<)
      cnt `shouldBe` 4

      cnt2 <- getCountWithCriterion buf 50 (:<=)
      cnt2 `shouldBe` 5

    specify "getCountWithCriterion at lower boundary (spaceAtBottom=False)" $ do
      buf <- mkBuffer 16 0 False
      mapM_ (append buf) [10, 20, 30, 40, 50]
      sort buf

      vec <- getVector buf
      forM_ [5..15] $ \i -> MUVector.unsafeWrite vec i 0

      cnt <- getCountWithCriterion buf 5 (:<)
      cnt `shouldBe` 0

      cnt2 <- getCountWithCriterion buf 10 (:<)
      cnt2 `shouldBe` 0

      cnt3 <- getCountWithCriterion buf 10 (:<=)
      cnt3 `shouldBe` 1

    specify "rank of value above max is correct with LowRanksAreAccurate" $ do
      -- LowRanksAreAccurate uses spaceAtBottom=False, which was the buggy path.
      sk <- mkReqSketch 12 LowRanksAreAccurate
      forM_ [1..50 :: Double] $ insert sk
      -- Rank of a value larger than everything should be 1.0 with (:<) criterion
      r <- rank sk 100
      r `shouldBe` 1.0

    specify "rank of value below min is 0 with LowRanksAreAccurate" $ do
      sk <- mkReqSketch 12 LowRanksAreAccurate
      forM_ [10..60 :: Double] $ insert sk
      r <- rank sk 5
      r `shouldBe` 0.0

    specify "ranks are correct for known values with LowRanksAreAccurate" $ do
      sk <- mkReqSketch 50 LowRanksAreAccurate
      -- With k=50 and 10 items, we're in exact mode (no compaction)
      let values = [5, 5, 5, 6, 6, 6, 7, 8, 8, 8 :: Double]
      mapM_ (insert sk) values
      -- With (<) criterion (default):
      --   rank(5) = 0/10 = 0.0 (nothing is < 5)
      --   rank(6) = 3/10 = 0.3 (three 5s are < 6)
      --   rank(7) = 6/10 = 0.6
      --   rank(8) = 7/10 = 0.7
      --   rank(9) = 10/10 = 1.0 (everything is < 9)
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
    -- The bug: growUntil only called grow once instead of looping,
    -- so merging a sketch with many levels into one with few levels
    -- would silently drop data from higher compactors.

    specify "merge into empty sketch preserves count from multi-level source" $ do
      -- Build a sketch with many items to force multiple compactor levels
      skBig <- mkReqSketch 6 HighRanksAreAccurate
      forM_ [1..2000 :: Double] $ insert skBig
      bigLevels <- fmap length . getCompactors $ skBig
      bigLevels `shouldSatisfy` (>= 3)

      -- Now merge into a fresh (1-level) sketch
      skSmall <- mkReqSketch 6 HighRanksAreAccurate
      _ <- merge skSmall skBig

      -- Without the fix, growUntil only added 1 level, so zipWithM_
      -- would silently skip higher-level compactors in skBig.
      -- The total count must match.
      mergedCount <- count skSmall
      mergedCount `shouldBe` 2000

      -- The retained items should also account for all compacted data
      smallLevels <- fmap length . getCompactors $ skSmall
      smallLevels `shouldSatisfy` (>= bigLevels)

    specify "merge sketch with 4+ levels into 1-level sketch" $ do
      skSrc <- mkReqSketch 6 HighRanksAreAccurate
      forM_ [1..5000 :: Double] $ insert skSrc
      srcLevels <- fmap length . getCompactors $ skSrc
      srcLevels `shouldSatisfy` (>= 4)

      skDst <- mkReqSketch 6 HighRanksAreAccurate
      insert skDst 0  -- just so it's not empty, but still 1 level
      dstLevelsBefore <- fmap length . getCompactors $ skDst
      dstLevelsBefore `shouldBe` 1

      _ <- merge skDst skSrc
      dstLevelsAfter <- fmap length . getCompactors $ skDst
      dstLevelsAfter `shouldSatisfy` (>= srcLevels)
      mergedCount <- count skDst
      mergedCount `shouldBe` 5001

  describe "Bug fix: max value comparison inverted in merge" $ do
    -- The bug: the merge function had (otherMax < thisMax) instead of
    -- (otherMax > thisMax), so it would update maxValue when the other
    -- sketch had a SMALLER max, and not update it when it had a LARGER max.

    specify "merge updates maximum when other has larger max" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [1..10 :: Double] $ insert sk1
      max1 <- REQ.maximum sk1
      max1 `shouldBe` 10.0

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [100..200 :: Double] $ insert sk2

      _ <- merge sk1 sk2
      mergedMax <- REQ.maximum sk1
      -- With the bug: max would be 10 (unchanged, because 200 > 10 failed
      -- the inverted condition 200 < 10). With the fix: max is 200.
      mergedMax `shouldBe` 200.0

    specify "merge does NOT update maximum when other has smaller max" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [100..200 :: Double] $ insert sk1

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [1..10 :: Double] $ insert sk2

      _ <- merge sk1 sk2
      mergedMax <- REQ.maximum sk1
      -- Max should stay at 200, not be overwritten with 10
      mergedMax `shouldBe` 200.0

    specify "merge with bug would have produced wrong max (both directions)" $ do
      sk1 <- mkReqSketch 12 HighRanksAreAccurate
      mapM_ (insert sk1) [1, 2, 3 :: Double]

      sk2 <- mkReqSketch 12 HighRanksAreAccurate
      mapM_ (insert sk2) [10, 20, 30 :: Double]

      _ <- merge sk1 sk2
      -- sk1.max should be 30 after merge
      mergedMax <- REQ.maximum sk1
      mergedMax `shouldBe` 30.0

      -- Also check min is correct for completeness
      mergedMin <- REQ.minimum sk1
      mergedMin `shouldBe` 1.0
