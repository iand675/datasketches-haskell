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

-- Run a batch of commands against the Java harness and return all output lines
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

-- Parse a double from Java output, handling "NaN"
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

-- REQ sketch: property tests comparing Haskell vs Java
reqCrossValidation :: FilePath -> Spec
reqCrossValidation harnessDir = do

  specify "REQ: exact mode count/min/max match Java (HighRanksAreAccurate, <)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 1 50) $
        Gen.double (Range.linearFrac 1 1000)
      let k = 50 :: Word32
      liftIO $ do
        -- Haskell
        sk <- REQ.mkReqSketch k REQ.HighRanksAreAccurate
        forM_ values $ REQ.insert sk
        hCount <- REQ.count sk
        hMin <- REQ.minimum sk
        hMax <- REQ.maximum sk

        -- Java (note: Java REQ uses float internally, not double)
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
        assertApproxEqual "min" 1e-4 hMin jMin
        assertApproxEqual "max" 1e-4 hMax jMax

  specify "REQ: exact mode ranks match Java (HighRanksAreAccurate, <)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 10 50) $
        Gen.double (Range.linearFrac 1 100)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        Gen.double (Range.linearFrac 0 110)
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
          unless (isNaN hr && isNaN jr) $
            assertApproxEqual ("rank of " ++ show qv) 0.05 hr jr

  specify "REQ: exact mode ranks match Java (LowRanksAreAccurate, <)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.linear 10 50) $
        Gen.double (Range.linearFrac 1 100)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        Gen.double (Range.linearFrac 0 110)
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
          unless (isNaN hr && isNaN jr) $
            assertApproxEqual ("rank of " ++ show qv) 0.05 hr jr

  specify "REQ: estimation mode rank bounds overlap Java (k=6, 200 items)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.singleton 200) $
        Gen.double (Range.linearFrac 1 1000)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
        Gen.double (Range.linearFrac 1 1000)
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

        -- In estimation mode, ranks won't match exactly since the two
        -- implementations use different random seeds. But both should
        -- be within the sketch's error bounds of the true rank.
        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $
            assertApproxEqual ("rank of " ++ show qv) 0.15 hr jr

-- KLL sketch cross-validation
kllCrossValidation :: FilePath -> Spec
kllCrossValidation harnessDir = do

  specify "KLL: count/min/max match Java" $ hedgehog $
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

  specify "KLL: exact mode ranks match Java (k=200, few items)" $ hedgehog $
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
          unless (isNaN hr && isNaN jr) $
            assertApproxEqual ("rank of " ++ show qv) 0.05 hr jr

  specify "KLL: estimation mode ranks within tolerance (k=200, 500 items)" $ hedgehog $
    H.property $ do
      values <- H.forAll $ Gen.list (Range.singleton 500) $
        Gen.double (Range.linearFrac 1 1000)
      queryValues <- H.forAll $ Gen.list (Range.linear 1 5) $
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

        -- Both are approximate with different random seeds and compaction
        -- strategies. With k=200, error ≈ 1.3%, but two independent
        -- implementations can differ by up to 2x the error bound.
        forM_ (zip3 queryValues hRanks jRanks) $ \(qv, hr, jr) ->
          unless (isNaN hr && isNaN jr) $
            assertApproxEqual ("rank of " ++ show qv) 0.10 hr jr

assertApproxEqual :: String -> Double -> Double -> Double -> IO ()
assertApproxEqual label tolerance actual expected =
  when (abs (actual - expected) > tolerance) $
    expectationFailure $ label ++ ": expected " ++ show expected
      ++ " +/- " ++ show tolerance
      ++ " but got " ++ show actual
      ++ " (delta=" ++ show (abs (actual - expected)) ++ ")"

-- | Run a Hedgehog property as an hspec test.
hedgehog :: H.Property -> IO ()
hedgehog prop = do
  result <- H.check prop
  unless result $ expectationFailure "Hedgehog property failed"
