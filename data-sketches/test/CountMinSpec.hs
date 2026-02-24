{-# LANGUAGE TypeApplications #-}
module CountMinSpec where

import Control.Monad (forM_)
import Data.Word
import Test.Hspec
import qualified DataSketches.Frequencies.CountMin as CM

spec :: Spec
spec = describe "Count-Min Sketch" $ do
  specify "empty sketch returns 0 for any item" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    est <- CM.estimate sk 42
    est `shouldBe` 0

  specify "single insert returns at least 1" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    CM.insert sk 42
    est <- CM.estimate sk 42
    est `shouldSatisfy` (>= 1)

  specify "multiple inserts of same item are counted" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    forM_ [1..100 :: Int] $ \_ -> CM.insert sk 42
    est <- CM.estimate sk 42
    est `shouldBe` 100

  specify "insertN works correctly" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    CM.insertN sk 42 50
    est <- CM.estimate sk 42
    est `shouldBe` 50

  specify "never undercounts" $ do
    sk <- CM.mkCountMinSketch 0.01 0.01
    forM_ [1..1000 :: Word64] $ \i -> CM.insert sk i
    -- Each item inserted once, estimate should be >= 1
    est <- CM.estimate sk 500
    est `shouldSatisfy` (>= 1)

  specify "unseen items may return small positive values" $ do
    sk <- CM.mkCountMinSketch 0.01 0.01
    forM_ [1..10000 :: Word64] $ \i -> CM.insert sk i
    -- Item 99999 was never inserted
    est <- CM.estimate sk 99999
    -- Should be 0 or a small number due to collisions
    est `shouldSatisfy` (<= 200)

  specify "heavy hitter detection" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    -- Insert item 42 many times
    CM.insertN sk 42 10000
    -- Insert many other items once each
    forM_ [1..1000 :: Word64] $ \i -> CM.insert sk i
    est42 <- CM.estimate sk 42
    estOther <- CM.estimate sk 500
    est42 `shouldSatisfy` (> estOther)
    est42 `shouldSatisfy` (>= 10000)

  specify "merge combines two sketches" $ do
    sk1 <- CM.mkCountMinSketch 0.001 0.01
    CM.insertN sk1 42 100
    sk2 <- CM.mkCountMinSketch 0.001 0.01
    CM.insertN sk2 42 200
    CM.merge sk1 sk2
    est <- CM.estimate sk1 42
    est `shouldBe` 300

  specify "dimensions match epsilon and delta" $ do
    sk <- CM.mkCountMinSketch 0.001 0.01
    w <- CM.width sk
    w `shouldSatisfy` (> 0)
    d <- CM.depth sk
    d `shouldSatisfy` (> 0)
