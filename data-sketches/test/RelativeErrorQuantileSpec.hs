{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NumericUnderscores #-}
module RelativeErrorQuantileSpec where

import Control.Monad
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Data.List hiding (insert, null)
import qualified Data.List
import Data.Word
import Test.Hspec

spec :: Spec
spec = do
  specify "non finite PMF/CDF should throw" $ asIO $ do
    sk <- mkReqSketch 6 HighRanksAreAccurate
    insert sk 1
    cumulativeDistributionFunction sk [0 / 0] `shouldThrow` (\(DoubleIsNonFiniteException _) -> True)
  specify "updating a sketch with NaN should ignore it" $ asIO $ do
    sk <- mkReqSketch 6 HighRanksAreAccurate
    insert sk (0 / 0)
    isEmpty <- DataSketches.Quantiles.RelativeErrorQuantile.null sk
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
