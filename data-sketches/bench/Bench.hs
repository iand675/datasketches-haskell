{-# OPTIONS_GHC -fno-full-laziness #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import Control.Monad (void)
import Criterion.Main
import Criterion.Types
import Data.Word
import Control.Monad.Primitive (PrimState)
import qualified Data.Vector.Storable as VS
import qualified DataSketches.Quantiles.RelativeErrorQuantile as REQ
import qualified DataSketches.Quantiles.KLL as KLL
import qualified DataSketches.Distinct.HyperLogLog as HLL
import qualified DataSketches.Frequencies.CountMin as CM

loopDouble :: (Double -> IO ()) -> Double -> Double -> IO ()
loopDouble f !lo !hi = go lo
  where
    go !i
      | i > hi = pure ()
      | otherwise = f i >> go (i + 1)
{-# INLINE loopDouble #-}

loopWord64 :: (Word64 -> IO ()) -> Word64 -> Word64 -> IO ()
loopWord64 f !lo !hi = go lo
  where
    go !i
      | i > hi = pure ()
      | otherwise = f i >> go (i + 1)
{-# INLINE loopWord64 #-}

loopDoubleStep :: (Double -> IO ()) -> Double -> Double -> Double -> IO ()
loopDoubleStep f !lo !hi !step = go lo
  where
    go !i
      | i > hi = pure ()
      | otherwise = f i >> go (i + step)
{-# INLINE loopDoubleStep #-}

doubleVec :: Int -> VS.Vector Double
doubleVec n = VS.generate n (fromIntegral . (+ 1))

word64Vec :: Int -> VS.Vector Word64
word64Vec n = VS.generate n (fromIntegral . (+ 1))

main :: IO ()
main = defaultMain
  [ bgroup "REQ"
    [ bgroup "insert"
      [ bench "100"    $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          loopDouble (REQ.insert sk) 1 100
      , bench "1000"   $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          loopDouble (REQ.insert sk) 1 1000
      , bench "10000"  $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          loopDouble (REQ.insert sk) 1 10000
      , bench "100000" $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          loopDouble (REQ.insert sk) 1 100000
      ]
    , bgroup "insertBatch"
      [ bench "100"    $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          REQ.insertBatch sk (doubleVec 100)
      , bench "1000"   $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          REQ.insertBatch sk (doubleVec 1000)
      , bench "10000"  $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          REQ.insertBatch sk (doubleVec 10000)
      , bench "100000" $ perRunEnv (REQ.mkReqSketch 6 REQ.HighRanksAreAccurate) $ \sk ->
          REQ.insertBatch sk (doubleVec 100000)
      ]
    , bgroup "rank"
      [ bench "100-queries-from-10000" $ perRunEnv (mkReqWith 10000) $ \sk ->
          loopDoubleStep (void . REQ.rank sk) 0 9900 100
      ]
    ]
  , bgroup "KLL"
    [ bgroup "insert"
      [ bench "100"    $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          loopDouble (KLL.insert sk) 1 100
      , bench "1000"   $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          loopDouble (KLL.insert sk) 1 1000
      , bench "10000"  $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          loopDouble (KLL.insert sk) 1 10000
      , bench "100000" $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          loopDouble (KLL.insert sk) 1 100000
      ]
    , bgroup "insertBatch"
      [ bench "100"    $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          KLL.insertBatch sk (doubleVec 100)
      , bench "1000"   $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          KLL.insertBatch sk (doubleVec 1000)
      , bench "10000"  $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          KLL.insertBatch sk (doubleVec 10000)
      , bench "100000" $ perRunEnv (KLL.mkKllSketch 200) $ \sk ->
          KLL.insertBatch sk (doubleVec 100000)
      ]
    , bgroup "rank"
      [ bench "100-queries-from-10000" $ perRunEnv (mkKllWith 10000) $ \sk ->
          loopDoubleStep (void . KLL.rank sk) 0 9900 100
      ]
    ]
  , bgroup "HLL"
    [ bgroup "insert"
      [ bench "1000"   $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          loopWord64 (HLL.insert sk) 1 1000
      , bench "10000"  $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          loopWord64 (HLL.insert sk) 1 10000
      , bench "100000" $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          loopWord64 (HLL.insert sk) 1 100000
      ]
    , bgroup "insertBatch"
      [ bench "1000"   $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          HLL.insertBatch sk (word64Vec 1000)
      , bench "10000"  $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          HLL.insertBatch sk (word64Vec 10000)
      , bench "100000" $ perRunEnv (HLL.mkHllSketch 12) $ \sk ->
          HLL.insertBatch sk (word64Vec 100000)
      ]
    ]
  , bgroup "CountMin"
    [ bgroup "insert"
      [ bench "10000"  $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          loopWord64 (CM.insert sk) 1 10000
      , bench "100000" $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          loopWord64 (CM.insert sk) 1 100000
      ]
    ]
  ]

mkReqWith :: Int -> IO (REQ.ReqSketch (PrimState IO))
mkReqWith n = do
  sk <- REQ.mkReqSketch 6 REQ.HighRanksAreAccurate
  loopDouble (REQ.insert sk) 1 (fromIntegral n)
  pure sk

mkKllWith :: Int -> IO (KLL.KllSketch (PrimState IO))
mkKllWith n = do
  sk <- KLL.mkKllSketch 200
  loopDouble (KLL.insert sk) 1 (fromIntegral n)
  pure sk
