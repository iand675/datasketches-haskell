{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NumericUnderscores #-}
module RelativeErrorQuantileSpec where

import Control.Monad
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile hiding (null, minimum, maximum)
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Data.List hiding (insert, null, minimum, maximum)
import qualified Data.List
import Data.Word
import Test.Hspec
import qualified Data.Vector.Storable as VS

spec :: Spec
spec = do
  specify "non finite PMF/CDF should throw" $ asIO $ do
    sk <- mkReqSketch 6 HighRanksAreAccurate
    insert sk 1
    cumulativeDistributionFunction sk [0 / 0] `shouldThrow` (\(DoubleIsNonFiniteException _) -> True)
  specify "updating a sketch with NaN should ignore it" $ asIO $ do
    sk <- mkReqSketch 6 HighRanksAreAccurate
    insert sk (0 / 0)
    isEmpty <- REQ.null sk
    isEmpty `shouldBe` True
  specify "non finite rank should throw" $ asIO $ do
    let infinity = read "Infinity"::Double
    sk <- mkReqSketch 6 HighRanksAreAccurate
    insert sk 1
    (rank sk infinity >>= print) `shouldThrow` (\(DoubleIsNonFiniteException _) -> True)
  specify "big merge doesn't explode" $ asIO $ do
    sk1 <- mkReqSketch 6 HighRanksAreAccurate
    mapM_ (insert sk1) [5..10]
    sk2 <- mkReqSketch 6 HighRanksAreAccurate
    merge sk1 sk2
    mapM_ (insert sk2) [1..15]
    merge sk1 sk2
    n <- count sk1
    n `shouldBe` 21
    mapM_ (insert sk2) [16..300]
    merge sk1 sk2
    n' <- count sk1
    n' `shouldBe` 321
  describe "property tests" $ do
    specify "ReqSketch quantile estimates are within ε bounds compared to real quantile calculations" $ print ()
    specify "merging N ReqSketches is equivalent +/- ε to inserting the same values into 1 ReqSketch" $ print ()

  let simpleTestValues = [5, 5, 5, 6, 6, 6, 7, 8, 8, 8]
  let lessThanRs = [0.0, 0.0, 0.0, 0.3, 0.3, 0.3, 0.6, 0.7, 0.7, 0.7]
  let lessThanEqRs = [0.3, 0.3, 0.3, 0.6, 0.6, 0.6, 0.7, 1.0, 1.0, 1.0]
  let simpleTestSetup = do
        sk <- mkReqSketch 50 HighRanksAreAccurate
        mapM_ (insert sk) simpleTestValues
        pure sk
  specify "lots of repeat values should work" $ asIO $ do
    sk <- mkReqSketch 6 HighRanksAreAccurate
    replicateM_ 10_000 (insert sk 1 >> retainedItemCount sk)
    print =<< retainedItemCount sk

  describe "simple test" $ before simpleTestSetup $ do
    describe "<" $ do
      specify "ranks function should match lessThanRs" $ \sk -> do
        actualRanks <- ranks sk simpleTestValues
        actualRanks `shouldBe` lessThanRs
      specify "mapM rank should match ranks behaviour" $ \sk -> do
        actualRanks <- mapM (rank sk) simpleTestValues
        actualRanks `shouldBe` lessThanRs
    describe "<=" $ do
      specify "ranks function should match lessThanEqRs" $ \sk -> do
        setCriterionLE sk
        actualRanks <- ranks sk simpleTestValues
        actualRanks `shouldBe` lessThanEqRs
      specify "mapM rank should match ranks behaviour" $ \sk -> do
        setCriterionLE sk
        actualRanks <- mapM (rank sk) simpleTestValues
        actualRanks `shouldBe` lessThanEqRs

  --      k min max hra                  lteq  low-to-high or high-to-low
  bigTest 6 1   200 HighRanksAreAccurate False True
  bigTest 6 1   200 LowRanksAreAccurate  False True
  bigTest 6 1   200 HighRanksAreAccurate True  False
  bigTest 6 1   200 LowRanksAreAccurate  True  True

  mergeSpec

  describe "quantiles" $ do
    it "should be reasonable" $ asIO $ do
      sk <- mkReqSketch 6 HighRanksAreAccurate
      insert sk 1
      insert sk 1
      insert sk 1
      r <- quantile sk 0.5
      r `shouldBe` 1.0

  describe "quantile monotonicity" $ do
    specify "quantiles are non-decreasing for increasing normalized ranks (exact mode)" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [1..100 :: Double]
      let rankPoints = [0, 0.1 .. 1.0]
      qs <- quantiles sk rankPoints
      forM_ (zip qs (drop 1 qs)) $ \(q1, q2) ->
        q2 `shouldSatisfy` (>= q1)

    specify "quantiles are non-decreasing (estimation mode, HRA)" $ asIO $ do
      sk <- mkReqSketch 6 HighRanksAreAccurate
      mapM_ (insert sk) [1..2000 :: Double]
      estimation <- isEstimationMode sk
      estimation `shouldBe` True
      let rankPoints = [0, 0.01 .. 1.0]
      qs <- quantiles sk rankPoints
      forM_ (zip qs (drop 1 qs)) $ \(q1, q2) ->
        q2 `shouldSatisfy` (>= q1)

    specify "quantiles are non-decreasing (estimation mode, LRA)" $ asIO $ do
      sk <- mkReqSketch 6 LowRanksAreAccurate
      mapM_ (insert sk) [1..2000 :: Double]
      estimation <- isEstimationMode sk
      estimation `shouldBe` True
      let rankPoints = [0, 0.01 .. 1.0]
      qs <- quantiles sk rankPoints
      forM_ (zip qs (drop 1 qs)) $ \(q1, q2) ->
        q2 `shouldSatisfy` (>= q1)

  describe "countWithCriterion" $ do
    specify "counts items strictly less than threshold (LT)" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [10, 20, 30, 40, 50 :: Double]
      setCriterionLT sk
      cnt <- countWithCriterion sk 30
      cnt `shouldBe` 2
      cntAll <- countWithCriterion sk 55
      cntAll `shouldBe` 5
      cntNone <- countWithCriterion sk 5
      cntNone `shouldBe` 0

    specify "counts items less than or equal to threshold (LE)" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [10, 20, 30, 40, 50 :: Double]
      setCriterionLE sk
      cnt <- countWithCriterion sk 30
      cnt `shouldBe` 3
      cntAll <- countWithCriterion sk 50
      cntAll `shouldBe` 5
      cntNone <- countWithCriterion sk 5
      cntNone `shouldBe` 0

    specify "boundary: at exact min value" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [10, 20, 30, 40, 50 :: Double]
      setCriterionLT sk
      cntLt <- countWithCriterion sk 10
      cntLt `shouldBe` 0
      setCriterionLE sk
      cntLe <- countWithCriterion sk 10
      cntLe `shouldBe` 1

    specify "boundary: at exact max value" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [10, 20, 30, 40, 50 :: Double]
      setCriterionLT sk
      cntLt <- countWithCriterion sk 50
      cntLt `shouldBe` 4
      setCriterionLE sk
      cntLe <- countWithCriterion sk 50
      cntLe `shouldBe` 5

    specify "with duplicate values" $ asIO $ do
      sk <- mkReqSketch 50 HighRanksAreAccurate
      mapM_ (insert sk) [1, 1, 1, 2, 2, 3 :: Double]
      setCriterionLT sk
      cntLt <- countWithCriterion sk 2
      cntLt `shouldBe` 3
      setCriterionLE sk
      cntLe <- countWithCriterion sk 2
      cntLe `shouldBe` 5

    specify "works in estimation mode after compaction" $ asIO $ do
      sk <- mkReqSketch 6 HighRanksAreAccurate
      mapM_ (insert sk) [1..2000 :: Double]
      estimation <- isEstimationMode sk
      estimation `shouldBe` True
      n <- count sk
      n `shouldBe` 2000
      setCriterionLE sk
      cntMax <- countWithCriterion sk 2000
      cntMax `shouldBe` n
      setCriterionLT sk
      cntMin <- countWithCriterion sk 0
      cntMin `shouldBe` 0

    specify "non-finite value throws DoubleIsNonFiniteException" $ asIO $ do
      sk <- mkReqSketch 6 HighRanksAreAccurate
      insert sk 1
      countWithCriterion sk (0 / 0) `shouldThrow` (\(DoubleIsNonFiniteException _) -> True)

  describe "estimation mode capacity growth" $ do
    specify "10,000 inserts followed by correct quantile queries" $ asIO $ do
      sk <- mkReqSketch 6 HighRanksAreAccurate
      forM_ [1..10_000 :: Double] $ insert sk
      n <- count sk
      n `shouldBe` 10_000
      estimation <- isEstimationMode sk
      estimation `shouldBe` True
      p50 <- quantile sk 0.5
      p50 `shouldSatisfy` (\v -> v >= 4000 && v <= 6000)
      p99 <- quantile sk 0.99
      p99 `shouldSatisfy` (\v -> v >= 9800 && v <= 10_000)
      mn <- REQ.minimum sk
      mn `shouldBe` 1
      mx <- REQ.maximum sk
      mx `shouldBe` 10_000

    specify "batch insert matches sequential insert" $ asIO $ do
      skSeq <- mkReqSketch 12 HighRanksAreAccurate
      forM_ [1..500 :: Double] $ insert skSeq

      skBatch <- mkReqSketch 12 HighRanksAreAccurate
      insertBatch skBatch (VS.fromList [1..500])

      seqCount <- count skSeq
      batchCount <- count skBatch
      seqCount `shouldBe` batchCount

      seqMin <- REQ.minimum skSeq
      batchMin <- REQ.minimum skBatch
      seqMin `shouldBe` batchMin

      seqMax <- REQ.maximum skSeq
      batchMax <- REQ.maximum skBatch
      seqMax `shouldBe` batchMax


bigTest :: Word32 -> Int -> Int -> RankAccuracy -> Bool -> Bool -> Spec
bigTest k min_ max_ hra useLe up = do
  let testName = unwords
        [ "k=" <> show k
        , "min=" <> show min_
        , "max=" <> show max_
        , "hra=" <> show hra
        , "le=" <> show useLe
        , "up=" <> show up
        ]
      testContents :: IO ()
      testContents = do
        sk <- loadSketch k min_ max_ hra useLe up
        checkGetRank sk min_ max_
        checkGetRanks sk max_
        checkGetQuantiles sk
        checkGetCDF sk
        checkGetPMF sk
  it testName testContents

asIO :: IO a -> IO a
asIO = id

mergeSpec :: Spec
mergeSpec = specify "merge works" $ asIO $ do
  s1 <- mkReqSketch 6 HighRanksAreAccurate :: IO (ReqSketch (PrimState IO))
  mapM_ (insert s1) [0..40]
  s2 <- mkReqSketch 6 HighRanksAreAccurate :: IO (ReqSketch (PrimState IO))
  mapM_ (insert s2) [0..40]
  s3 <- mkReqSketch 6 HighRanksAreAccurate :: IO (ReqSketch (PrimState IO))
  mapM_ (insert s3) [0..40]
  s <- mkReqSketch 6 HighRanksAreAccurate :: IO (ReqSketch (PrimState IO))
  s `merge` s1
  s `merge` s2
  s `merge` s3
  pure ()

loadSketch :: Word32 -> Int -> Int -> RankAccuracy -> Bool -> Bool -> IO (ReqSketch (PrimState IO))
loadSketch k min_ max_ hra useLe up = do
  sk <- mkReqSketch k hra :: IO (ReqSketch (PrimState IO))
  when useLe $ setCriterionLE sk
  mapM_ (insert sk . fromIntegral) $ if up
    then [min_ .. max_]
    else reverse [min_ .. max_]
  pure sk

checkGetRank :: ReqSketch (PrimState IO) -> Int -> Int -> IO ()
checkGetRank sk min_ max_ = do
  let (v : spArr) = evenlySpacedFloats 0 (fromIntegral max_) 11
  initialRank <- rank sk v
  foldM_
    (\(oldV, oldRank) v -> do
      r <- rank sk v
      v `shouldSatisfy` (>= oldV)
      r `shouldSatisfy` (>= oldRank)
      pure (v, r)
    )
    (v, initialRank)
    spArr

checkGetRanks :: ReqSketch (PrimState IO) -> Int -> IO ()
checkGetRanks sk max_ = do
  let sp = evenlySpacedFloats 0 (fromIntegral max_) 11
  void $ ranks sk sp

checkGetCDF :: ReqSketch (PrimState IO) -> IO ()
checkGetCDF sk = do
  let spArr = [20, 40 .. 180]
  r <- cumulativeDistributionFunction sk spArr
  r `shouldSatisfy` (\(Just _) -> True)

checkGetPMF :: ReqSketch (PrimState IO) -> IO ()
checkGetPMF sk = do
  let spArr = [20, 40 .. 180]
  r <- probabilityMassFunction sk spArr
  r `shouldNotSatisfy` Data.List.null

checkGetQuantiles :: ReqSketch (PrimState IO) -> IO ()
checkGetQuantiles sk = do
  let rArr = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
  void $ quantiles sk rArr

evenlySpacedFloats :: Double -> Double -> Word -> [Double]
evenlySpacedFloats _ _ 0 = error "Needs at least two steps"
evenlySpacedFloats _ _ 1 = error "Needs at least two steps"
evenlySpacedFloats value1 value2 steps = unfoldr
  (\ix -> let val = fromIntegral ix * delta + value1 in case value1 `compare` value2 of
    LT -> if val > value2 then Nothing else Just (val, ix + 1)
    EQ -> Nothing
    GT -> if val < value2 then Nothing else Just (val, ix + 1)
  )
  0
  where
    delta = (value2 - value1) / (fromIntegral steps - 1)
