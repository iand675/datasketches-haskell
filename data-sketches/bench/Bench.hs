{-# OPTIONS_GHC -fno-full-laziness #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import Control.Monad (forM_)
import Criterion.Main
import Criterion.Types
import Data.Word
import Control.Monad.Primitive (PrimState)
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
import qualified DataSketches.Quantiles.KLL as KLL
import qualified DataSketches.Distinct.HyperLogLog as HLL
import qualified DataSketches.Frequencies.CountMin as CM

main :: IO ()
main = defaultMain
  [ bgroup "REQ"
    [ bgroup "insert"
      [ bench "100"    $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          forM_ [1..100 :: Double] $ REQ.insert sk
      , bench "1000"   $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          forM_ [1..1000 :: Double] $ REQ.insert sk
      , bench "10000"  $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          forM_ [1..10000 :: Double] $ REQ.insert sk
      , bench "100000" $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          forM_ [1..100000 :: Double] $ REQ.insert sk
      ]
    , bgroup "rank"
      [ bench "100-queries-from-10000" $ perRunEnv (mkReqWith 10000) $ \sk ->
          forM_ [0,100..9900 :: Double] $ REQ.rank sk
      ]
    ]
  , bgroup "KLL"
    [ bgroup "insert"
      [ bench "100"    $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          forM_ [1..100 :: Double] $ KLL.insert sk
      , bench "1000"   $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          forM_ [1..1000 :: Double] $ KLL.insert sk
      , bench "10000"  $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          forM_ [1..10000 :: Double] $ KLL.insert sk
      , bench "100000" $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          forM_ [1..100000 :: Double] $ KLL.insert sk
      ]
    , bgroup "rank"
      [ bench "100-queries-from-10000" $ perRunEnv (mkKllWith 10000) $ \sk ->
          forM_ [0,100..9900 :: Double] $ KLL.rank sk
      ]
    ]
  , bgroup "HLL"
    [ bgroup "insert"
      [ bench "1000"   $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          forM_ [1..1000 :: Word64] $ HLL.insert sk
      , bench "10000"  $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          forM_ [1..10000 :: Word64] $ HLL.insert sk
      , bench "100000" $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          forM_ [1..100000 :: Word64] $ HLL.insert sk
      ]
    ]
  , bgroup "CountMin"
    [ bgroup "insert"
      [ bench "10000"  $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          forM_ [1..10000 :: Word64] $ CM.insert sk
      , bench "100000" $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          forM_ [1..100000 :: Word64] $ CM.insert sk
      ]
    ]
  ]

mkReqWith :: Int -> IO (REQ.ReqSketch (PrimState IO))
mkReqWith n = do
  sk <- REQ.mkReqSketch 6 REQ.HighRanksAreAccurate
  forM_ [1..fromIntegral n :: Double] $ REQ.insert sk
  pure sk

mkKllWith :: Int -> IO (KLL.KllSketch (PrimState IO))
mkKllWith n = do
  sk <- KLL.mkKllSketch 200
  forM_ [1..fromIntegral n :: Double] $ KLL.insert sk
  pure sk
