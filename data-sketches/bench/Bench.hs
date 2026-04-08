{-# OPTIONS_GHC -fno-full-laziness #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

import Control.Monad (void, forM_)
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
    , bgroup "merge"
      [ bench "10000-into-10000" $ perRunEnv (mkKllPair 10000) $ \(a, b) ->
          KLL.merge a b
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
    , bgroup "estimate"
      [ bench "p=12-after-10000" $ perRunEnv (mkHllWith 12 10000) $ \sk ->
          void (HLL.estimate sk)
      , bench "p=14-after-10000" $ perRunEnv (mkHllWith 14 10000) $ \sk ->
          void (HLL.estimate sk)
      , bench "p=12-after-100000" $ perRunEnv (mkHllWith 12 100000) $ \sk ->
          void (HLL.estimate sk)
      ]
    , bgroup "merge"
      [ bench "p=12" $ perRunEnv (mkHllPair 12 10000) $ \(a, b) ->
          HLL.merge a b
      , bench "p=14" $ perRunEnv (mkHllPair 14 10000) $ \(a, b) ->
          HLL.merge a b
      ]
    ]
  , bgroup "CountMin"
    [ bgroup "insert"
      [ bench "10000"  $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          loopWord64 (CM.insert sk) 1 10000
      , bench "100000" $ perRunEnv (CM.mkCountMinSketch 0.001 0.01) $ \sk ->
          loopWord64 (CM.insert sk) 1 100000
      ]
    , bgroup "estimate"
      [ bench "1000-queries" $ perRunEnv (mkCmsWith 100000) $ \sk ->
          loopWord64 (void . CM.estimate sk) 1 1000
      ]
    , bgroup "merge"
      [ bench "e=0.001-d=0.01" $ perRunEnv (mkCmsPair 100000) $ \(a, b) ->
          CM.merge a b
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

mkKllPair :: Int -> IO (KLL.KllSketch (PrimState IO), KLL.KllSketch (PrimState IO))
mkKllPair n = do
  a <- KLL.mkKllSketch 200
  b <- KLL.mkKllSketch 200
  loopDouble (KLL.insert a) 1 (fromIntegral n)
  loopDouble (KLL.insert b) (fromIntegral n + 1) (fromIntegral (n * 2))
  pure (a, b)

mkHllWith :: Int -> Int -> IO (HLL.HllSketch (PrimState IO))
mkHllWith p n = do
  sk <- HLL.mkHllSketch p
  loopWord64 (HLL.insert sk) 1 (fromIntegral n)
  pure sk

mkHllPair :: Int -> Int -> IO (HLL.HllSketch (PrimState IO), HLL.HllSketch (PrimState IO))
mkHllPair p n = do
  a <- HLL.mkHllSketch p
  b <- HLL.mkHllSketch p
  loopWord64 (HLL.insert a) 1 (fromIntegral n)
  loopWord64 (HLL.insert b) (fromIntegral n + 1) (fromIntegral (n * 2))
  pure (a, b)

mkCmsWith :: Int -> IO (CM.CountMinSketch (PrimState IO))
mkCmsWith n = do
  sk <- CM.mkCountMinSketch 0.001 0.01
  loopWord64 (CM.insert sk) 1 (fromIntegral n)
  pure sk

mkCmsPair :: Int -> IO (CM.CountMinSketch (PrimState IO), CM.CountMinSketch (PrimState IO))
mkCmsPair n = do
  a <- CM.mkCountMinSketch 0.001 0.01
  b <- CM.mkCountMinSketch 0.001 0.01
  loopWord64 (CM.insert a) 1 (fromIntegral n)
  loopWord64 (CM.insert b) (fromIntegral n + 1) (fromIntegral (n * 2))
  pure (a, b)
