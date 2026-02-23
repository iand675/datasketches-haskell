{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
module CrossValidationSpec where

import Control.Monad (forM_, when, unless)
import Control.Monad.IO.Class (liftIO)
import Data.Char (isSpace)
import Data.Word
import System.Directory (doesFileExist)
import System.IO (hFlush, hClose, hSetBuffering, BufferMode(..), hPutStrLn, hGetLine)
import System.Process
import Test.Hspec
import qualified Hedgehog as H
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
import qualified DataSketches.Quantiles.KLL as KLL

findHarnessDir :: IO (Maybe FilePath)
findHarnessDir = do
  let candidates = ["java-harness", "../java-harness"]
  go candidates
  where
    go [] = pure Nothing
    go (d:ds) = do
      exists <- doesFileExist (d ++ "/SketchHarness.class")
      if exists then pure (Just d) else go ds

javaClasspathFor :: FilePath -> String
javaClasspathFor dir = dir ++ ":" ++ dir ++ "/lib/*"

runJavaHarness :: FilePath -> [String] -> IO [String]
runJavaHarness harnessDir commands = do
  let cp = CreateProcess
        { cmdspec = RawCommand "java" ["-cp", javaClasspathFor harnessDir, "SketchHarness"]
        , cwd = Nothing
        , env = Nothing
        , std_in = CreatePipe
        , std_out = CreatePipe
        , std_err = Inherit
        , close_fds = False
        , create_group = False
        , delegate_ctlc = False
        , detach_console = False
        , create_new_console = False
        , new_session = False
        , child_group = Nothing
        , child_user = Nothing
        , use_process_jobs = False
        }
  (Just hin, Just hout, _, ph) <- createProcess cp
  hSetBuffering hin LineBuffering
  hSetBuffering hout LineBuffering

  forM_ commands $ \cmd -> hPutStrLn hin cmd
  hFlush hin
  hClose hin

  let readUntilDone acc = do
        line <- hGetLine hout
        if line == "DONE"
          then pure (reverse acc)
          else readUntilDone (line : acc)

  results <- readUntilDone []
  _ <- waitForProcess ph
  pure results

parseJavaDouble :: String -> Double
parseJavaDouble s
  | s == "NaN" = 0/0
  | otherwise = read (trim s)
  where trim = reverse . dropWhile isSpace . reverse . dropWhile isSpace

spec :: Spec
spec = do
  mDir <- runIO findHarnessDir
  case mDir of
    Nothing ->
      specify "Java harness not found (skipping cross-validation)" $ pendingWith
        "Compile java-harness/SketchHarness.java first"
    Just harnessDir -> do
      describe "REQ Sketch cross-validation with Java" $
        reqCrossValidation harnessDir
      describe "KLL Sketch cross-validation with Java" $
        kllCrossValidation harnessDir

-- | Generate integer-valued doubles in a range. These are exactly
-- representable in both float and double, eliminating precision as a
-- source of disagreement between the Java (float) and Haskell (double)
-- REQ sketches.
genIntDouble :: H.Range Int -> H.Gen Double
genIntDouble r = fromIntegral <$> Gen.int r

-- REQ sketch: property tests comparing Haskell vs Java.
--
-- Java REQ uses float (32-bit); Haskell uses Double (64-bit).
-- We use integer-valued doubles so both representations are identical
-- and exact-mode results must match exactly.
reqCrossValidation :: FilePath -> Spec
reqCrossValidation harnessDir = do

  specify "REQ: exact mode count/min/max match Java (HighRanksAreAccurate)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 1 50) $
        genIntDouble (Range.linear 1 1000)
      let k = 50 :: Word32
      liftIO $ do
        sk <- REQ.mkReqSketch k REQ.HighRanksAreAccurate
        forM_ values $ REQ.insert sk
        hCount <- REQ.count sk
        hMin <- REQ.minimum sk
        hMax <- REQ.maximum sk

        let jCmds =
              [ "REQ " ++ show k ++ " hra lt"
              , "INSERT " ++ unwords (fmap show values)
              , "COUNT"
              , "MIN"
              , "MAX"
              , "END"
              ]
        jResults <- runJavaHarness harnessDir jCmds
        let jCount = read @Word64 (jResults !! 0)
            jMin = parseJavaDouble (jResults !! 1)
            jMax = parseJavaDouble (jResults !! 2)

        hCount `shouldBe` jCount
        hMin `shouldBe` jMin
        hMax `shouldBe` jMax

  specify "REQ: exact mode ranks match Java exactly (HighRanksAreAccurate, <)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 10 50) $
        genIntDouble (Range.linear 1 200)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        genIntDouble (Range.linear 0 210)
      let k = 50 :: Word32
      liftIO $ do
        sk <- REQ.mkReqSketch k REQ.HighRanksAreAccurate
        forM_ values $ REQ.insert sk
        hRanks <- mapM (REQ.rank sk) queryValues

        let jCmds =
              [ "REQ " ++ show k ++ " hra lt"
              , "INSERT " ++ unwords (fmap show values)
              ] ++
              fmap (\q -> "RANK " ++ show q) queryValues ++
              [ "END" ]
        jResults <- runJavaHarness harnessDir jCmds
        let jRanks = fmap parseJavaDouble jResults

        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $ do
            hr `shouldBe` jr

  specify "REQ: exact mode ranks match Java exactly (LowRanksAreAccurate, <)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 10 50) $
        genIntDouble (Range.linear 1 200)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        genIntDouble (Range.linear 0 210)
      let k = 50 :: Word32
      liftIO $ do
        sk <- REQ.mkReqSketch k REQ.LowRanksAreAccurate
        forM_ values $ REQ.insert sk
        hRanks <- mapM (REQ.rank sk) queryValues

        let jCmds =
              [ "REQ " ++ show k ++ " lra lt"
              , "INSERT " ++ unwords (fmap show values)
              ] ++
              fmap (\q -> "RANK " ++ show q) queryValues ++
              [ "END" ]
        jResults <- runJavaHarness harnessDir jCmds
        let jRanks = fmap parseJavaDouble jResults

        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $ do
            hr `shouldBe` jr

  specify "REQ: estimation mode ranks are both valid approximations (k=6, 200 items)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.singleton 200) $
        genIntDouble (Range.linear 1 1000)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 3) $
        genIntDouble (Range.linear 1 1000)
      let k = 6 :: Word32
      liftIO $ do
        sk <- REQ.mkReqSketch k REQ.HighRanksAreAccurate
        forM_ values $ REQ.insert sk
        hRanks <- mapM (REQ.rank sk) queryValues

        let jCmds =
              [ "REQ " ++ show k ++ " hra lt"
              , "INSERT " ++ unwords (fmap show values)
              ] ++
              fmap (\q -> "RANK " ++ show q) queryValues ++
              [ "END" ]
        jResults <- runJavaHarness harnessDir jCmds
        let jRanks = fmap parseJavaDouble jResults

        -- In estimation mode, each implementation made independent random
        -- compaction decisions (different RNG seeds), so the retained items
        -- differ. Both ranks should approximate the true rank, and we check
        -- that the two approximations don't diverge beyond 2x the sketch's
        -- error bound (~14% per side for k=6).
        let n = fromIntegral (length values) :: Double
        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $ do
            let trueRank = fromIntegral (length (filter (< qv) values)) / n
            assertWithinBound ("Haskell rank of " ++ show qv) 0.15 hr trueRank
            assertWithinBound ("Java rank of " ++ show qv) 0.15 jr trueRank

-- KLL sketch cross-validation.
--
-- Both Java and Haskell KLL use doubles, so all values are identical
-- in both representations. In exact mode results must match exactly.
kllCrossValidation :: FilePath -> Spec
kllCrossValidation harnessDir = do

  specify "KLL: exact mode count/min/max match Java exactly" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 1 100) $
        Gen.double (Range.linearFrac 1 1000)
      let k = 200
      liftIO $ do
        sk <- KLL.mkKllSketch k
        forM_ values $ KLL.insert sk
        hCount <- KLL.count sk
        hMin <- KLL.minimum sk
        hMax <- KLL.maximum sk

        let jCmds =
              [ "KLL " ++ show k
              , "INSERT " ++ unwords (fmap show values)
              , "COUNT"
              , "MIN"
              , "MAX"
              , "END"
              ]
        jResults <- runJavaHarness harnessDir jCmds
        let jCount = read @Word64 (jResults !! 0)
            jMin = parseJavaDouble (jResults !! 1)
            jMax = parseJavaDouble (jResults !! 2)

        hCount `shouldBe` jCount
        hMin `shouldBe` jMin
        hMax `shouldBe` jMax

  specify "KLL: exact mode ranks match Java exactly (k=200, few items)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 10 50) $
        Gen.double (Range.linearFrac 1 100)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        Gen.double (Range.linearFrac 0 110)
      let k = 200
      liftIO $ do
        sk <- KLL.mkKllSketch k
        forM_ values $ KLL.insert sk
        hRanks <- mapM (KLL.rank sk) queryValues

        let jCmds =
              [ "KLL " ++ show k
              , "INSERT " ++ unwords (fmap show values)
              ] ++
              fmap (\q -> "RANK " ++ show q) queryValues ++
              [ "END" ]
        jResults <- runJavaHarness harnessDir jCmds
        let jRanks = fmap parseJavaDouble jResults

        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $ do
            hr `shouldBe` jr

  specify "KLL: estimation mode ranks are both valid approximations (k=200, 500 items)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.singleton 500) $
        Gen.double (Range.linearFrac 1 1000)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 3) $
        Gen.double (Range.linearFrac 1 1000)
      let k = 200
      liftIO $ do
        sk <- KLL.mkKllSketch k
        forM_ values $ KLL.insert sk
        hRanks <- mapM (KLL.rank sk) queryValues

        let jCmds =
              [ "KLL " ++ show k
              , "INSERT " ++ unwords (fmap show values)
              ] ++
              fmap (\q -> "RANK " ++ show q) queryValues ++
              [ "END" ]
        jResults <- runJavaHarness harnessDir jCmds
        let jRanks = fmap parseJavaDouble jResults

        -- Different compaction decisions from different RNGs. Verify both
        -- approximate the true rank rather than comparing to each other.
        let n = fromIntegral (length values) :: Double
        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $ do
            let trueRank = fromIntegral (length (filter (< qv) values)) / n
            assertWithinBound ("Haskell rank of " ++ show qv) 0.05 hr trueRank
            assertWithinBound ("Java rank of " ++ show qv) 0.05 jr trueRank

-- | Assert that actual is within tolerance of expected.
assertWithinBound :: String -> Double -> Double -> Double -> IO ()
assertWithinBound label tolerance actual expected =
  when (abs (actual - expected) > tolerance) $
    expectationFailure $ label ++ ": expected " ++ show expected
      ++ " +/- " ++ show tolerance
      ++ " but got " ++ show actual
      ++ " (delta=" ++ show (abs (actual - expected)) ++ ")"

hedgehog :: H.Property -> IO ()
hedgehog prop = do
  result <- H.check prop
  unless result $ expectationFailure "Hedgehog property failed"
