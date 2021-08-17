module DoubleBufferSpec where

import DataSketches.Quantiles.RelativeErrorQuantile
import qualified Data.Vector.Unboxed as UV

spec :: Spec
spec = do
  describe "Merge Sort" $ do
    let mergeSortTestImpl direction = do
          buf <- mkBuffer 16 0 direction
          forM [0..31] (append buf)
          buf2 <- copyBuffer buf
          mergeSortIn buf buf2
          v <- getVector buf
          UV.freeze v
    it "works upwards" $ do
      v <- mergeSortTestImpl False
      UV.fromList (sort ([0..31] ++ [0..31])) @=? v
    it "works downwards" $ do
      v <- mergeSortTestImpl True
      UV.fromList (sort ([0..31] ++ [0..31])) @=? v
  {-
  describe "getEvensOrOdds" $ do
    checkGetEvensOrOdds False False
    checkGetEvensOrOdds False True
    checkGetEvensOrOdds True False
    checkGetEvensOrOdds True True
  -}

{-
checkGetEvensOrOdds :: Bool -> Bool -> _
checkGetEvensOrOdds odds spaceAtBottom = 
  it ("works for odds=" ++ show odds ++ " spaceAtBottom=" ++ show spaceAtBottom) $ do
    buf <- mkBuffer cap 0 spaceAtBottom
    forM_ [0..cap `div` 2] (append buf . fromIntegral)
    getEvensOrOdds buf 0 (cap `div` 2) odds
  where
    cap = 16
-}