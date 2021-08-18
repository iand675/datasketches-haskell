module RelativeErrorQuantileSpec where

import Control.Monad
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Data.List
import Data.Proxy
import GHC.TypeLits
import Test.Hspec

spec :: Spec
spec = do
  specify "non finite PMF/CDF should throw" $ do
    rs <- mkReqSketch @6 HighRanksAreAccurate
    update sk 1
    getCDF sk [0 / 0] `shouldThrow` anyException
  specify "updating a sketch with NaN should ignore it" $ do
    rs <- mkReqSketch @6 HighRanksAreAccurate
    update sk (0 / 0)
    isEmpty <- null rs
    isEmpty `shouldBe` True
  specify "non finite rank should throw" $ do
    let infinity = (read "Infinity")::Double
    rs <- mkReqSketch @6 HighRanksAreAccurate
    update sk 1
    rank sk infinity `shouldThrow` anyException
  specify "big merge doesn't explode" $ do
    sk1 <- mkReqSketch @6 HighRanksAreAccurate
    mapM_ (update sk1) [5..10]
    sk2 <- mkReqSketch @6 HighRanksAreAccurate
    merge sk1 sk2
    mapM_ (update sk2) [1..15]
    merge sk1 sk2
    n <- getN sk1
    n `shouldBe` 20
    mapM_ (update sk2) [16..300]
    merge sk1 sk2
  specify "simple test" $ do
    sk <- mkReqSketch @20 HighRanksAreAccurate
    let vs = [5, 5, 5, 6, 6, 6, 7, 8, 8, 8]
    let lessThanRs = [0.0, 0.0, 0.0, 0.3, 0.3, 0.3, 0.6, 0.7, 0.7, 0.7]
    mapM_ (update sk) 
    do
      actualRanks <- ranks vs
      actualRanks `shouldBe` lessThanRs
    do
      actualRanks <- mapM rank vs
      actualRanks `shouldBe` lessThanRs
    let sk' = sk { criterion = (:<=) }
    let lessThanEqRs = [0.3, 0.3, 0.3, 0.6, 0.6, 0.6, 0.7, 1.0, 1.0, 1.0]
    do
      actualRanks <- ranks vs'
      actualRanks `shouldBe` lessThanEqRs
    do
      actualRanks <- mapM rank vs'
      actualRanks `shouldBe` lessThanEqRs







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
        checkAux sk
        checkGetRank sk min_ max_
        -- checkGetRanks sk, max
        checkGetQuantiles sk
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

checkAux :: ReqSketch n (PrimState IO) -> IO ()
checkAux sk = do
  aux <- auxilliary sk
  totalCount <- computeTotalRetainedItems sk
  initialRow <- getRow aux 0
  otherRows <- mapM (getRow aux) [1..totalCount]
  foldM
    (\lastRow thisRow -> do
      item thisRow `shouldSatisfy` (>= item lastRow)
      weight thisRow `shouldSatisfy` (>= weight lastRow)
    )
    initialRow
    otherRows

checkGetRank :: KnownNat n => ReqSketch n (PrimState IO) -> Int -> Int -> IO ()
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

checkGetRankConcreteExample :: RankAccuracy -> Criterion -> IO ()
checkGetRankConcreteExample ra crit = do
  sk <- loadSketch (Proxy :: Proxy 12) 1 1000 ra crit
  rLB <- rankLowerBound 0.5 1
  rLB `shouldSatisfy` (> 0)
  rLB <- case ra of
    HighRanksAreAccurate -> rankLowerBound sk (995 / 1000) 1
    LowRanksAreAccurate -> rankLowerBound sk (5 / 1000) 1
  rLB `shouldSatisfy` (> 0)
  rUB <- rankUpperBound 0.5 1
  rUB `shouldSatisfy` (> 0)
  rUB <- case ra of
    HighRanksAreAccurate -> rankUpperBound sk (995 / 1000) 1
    LowRanksAreAccurate -> rankUpperBound sk (5 / 1000) 1
  rUB `shouldSatisfy` (> 0)
  void $ ranks sk [5, 100]





checkGetQuantiles 
  :: KnownNat n 
  => ReqSketch n (PrimState IO) 
  -> IO ()
checkGetQuantiles sk = do
  let rArr = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
  -- nothing getting checked here apparently, guess the thing not
  -- exploding is sufficient.
  qOut <- quantiles sk rArr
  pure ()

-- | Returns a float array of evenly spaced values between value1 and value2 inclusive.
-- If value2 > value1, the resulting sequence will be increasing.
-- If value2 < value1, the resulting sequence will be decreasing.
-- value1 will be in index 0 of the returned array
-- value2 will be in the highest index of the returned array
-- valu3 is the total number of values including value1 and value2. Must be 2 or greater.
-- returns a float array of evenly spaced values between value1 and value2 inclusive.
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
