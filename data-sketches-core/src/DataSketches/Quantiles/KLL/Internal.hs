{-# LANGUAGE RankNTypes #-}
module DataSketches.Quantiles.KLL.Internal
  ( KllSketch(..)
  , mkKllSketch
  , kllInsert
  , kllCount
  , kllMinimum
  , kllMaximum
  , kllQuantile
  , kllRank
  , kllMerge
  , kllRetainedItems
  , kllIsEmpty
  ) where

import Control.DeepSeq (NFData(..))
import Control.Monad (when, unless, forM_)
import Control.Monad.Primitive
import Data.Bits (shiftL, shiftR)
import Data.Primitive.MutVar
import Data.Word
import qualified Data.Vector.Unboxed as UVector
import qualified Data.Vector.Unboxed.Mutable as MUVector
import qualified Data.Vector as Vector
import Data.Vector.Algorithms.Intro (sort)
import System.Random.MWC (Gen, uniform, create)
import DataSketches.Core.Internal.URef

minLevelSize :: Int
minLevelSize = 2

defaultM :: Int
defaultM = 8

data KllLevel s = KllLevel
  { klBuffer :: {-# UNPACK #-} !(MutVar s (MUVector.MVector s Double))
  , klCount :: {-# UNPACK #-} !(URef s Int)
  }

data KllSketch s = KllSketch
  { kllK :: !Word32
  , kllTotalN :: {-# UNPACK #-} !(URef s Word64)
  , kllMinValue :: {-# UNPACK #-} !(URef s Double)
  , kllMaxValue :: {-# UNPACK #-} !(URef s Double)
  , kllLevels :: {-# UNPACK #-} !(MutVar s (Vector.Vector (KllLevel s)))
  , kllRng :: {-# UNPACK #-} !(Gen s)
  }

instance NFData (KllSketch s) where rnf !_ = ()

mkLevel :: PrimMonad m => Int -> m (KllLevel (PrimState m))
mkLevel capacity = do
  buf <- MUVector.new (max capacity minLevelSize)
  KllLevel <$> newMutVar buf <*> newURef 0

levelCount :: PrimMonad m => KllLevel (PrimState m) -> m Int
levelCount = readURef . klCount
{-# INLINE levelCount #-}

levelBuffer :: PrimMonad m => KllLevel (PrimState m) -> m (MUVector.MVector (PrimState m) Double)
levelBuffer = readMutVar . klBuffer
{-# INLINE levelBuffer #-}

levelAppend :: PrimMonad m => KllLevel (PrimState m) -> Double -> m ()
levelAppend lvl val = do
  cnt <- readURef (klCount lvl)
  buf <- readMutVar (klBuffer lvl)
  let cap = MUVector.length buf
  buf' <- if cnt >= cap
    then do
      newBuf <- MUVector.grow buf cap
      writeMutVar (klBuffer lvl) newBuf
      pure newBuf
    else pure buf
  MUVector.unsafeWrite buf' cnt val
  writeURef (klCount lvl) (cnt + 1)
{-# INLINE levelAppend #-}

levelClear :: PrimMonad m => KllLevel (PrimState m) -> m ()
levelClear lvl = writeURef (klCount lvl) 0
{-# INLINE levelClear #-}

-- Capacity of level h given numLevels total.
-- depth = numLevels - 1 - h (counted from top)
-- capacity = max(minLevelSize, round(k * (2/3)^depth))
levelCapacity :: Word32 -> Int -> Int -> Int
levelCapacity k numLevels h = max minLevelSize cap
  where
    depth = numLevels - 1 - h
    cap = round (fromIntegral k * (2.0/3.0 :: Double) ^^ depth)

totalCapacity :: Word32 -> Int -> Int
totalCapacity k numLevels =
  let go !acc !h
        | h >= numLevels = acc
        | otherwise = go (acc + levelCapacity k numLevels h) (h + 1)
  in go 0 0

-- | Create a new KLL sketch.
-- k controls accuracy vs space. Default 200 gives ~1.3% error.
-- Must satisfy k >= 8.
mkKllSketch :: PrimMonad m => Word32 -> m (KllSketch (PrimState m))
mkKllSketch k = do
  unless (k >= 8) $ error "KLL sketch: k must be >= 8"
  lvl0 <- mkLevel (fromIntegral k)
  KllSketch k
    <$> newURef 0
    <*> newURef (0/0)
    <*> newURef (0/0)
    <*> newMutVar (Vector.singleton lvl0)
    <*> create

kllIsEmpty :: PrimMonad m => KllSketch (PrimState m) -> m Bool
kllIsEmpty sk = (== 0) <$> readURef (kllTotalN sk)

kllCount :: PrimMonad m => KllSketch (PrimState m) -> m Word64
kllCount = readURef . kllTotalN

kllMinimum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMinimum = readURef . kllMinValue

kllMaximum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMaximum = readURef . kllMaxValue

kllRetainedItems :: PrimMonad m => KllSketch (PrimState m) -> m Int
kllRetainedItems sk = do
  lvls <- readMutVar (kllLevels sk)
  Vector.foldM (\acc lvl -> (+ acc) <$> levelCount lvl) 0 lvls

getNumLevels :: PrimMonad m => KllSketch (PrimState m) -> m Int
getNumLevels = fmap Vector.length . readMutVar . kllLevels

-- | Insert a value into the sketch.
kllInsert :: PrimMonad m => KllSketch (PrimState m) -> Double -> m ()
kllInsert sk val = do
  unless (isNaN val) $ do
    empty <- kllIsEmpty sk
    if empty
      then do
        writeURef (kllMinValue sk) val
        writeURef (kllMaxValue sk) val
      else do
        mn <- readURef (kllMinValue sk)
        mx <- readURef (kllMaxValue sk)
        when (val < mn) $ writeURef (kllMinValue sk) val
        when (val > mx) $ writeURef (kllMaxValue sk) val
    lvls <- readMutVar (kllLevels sk)
    levelAppend (Vector.head lvls) val
    modifyURef (kllTotalN sk) (+ 1)
    compressIfNeeded sk

compressIfNeeded :: PrimMonad m => KllSketch (PrimState m) -> m ()
compressIfNeeded sk = do
  retained <- kllRetainedItems sk
  numLvls <- getNumLevels sk
  let cap = totalCapacity (kllK sk) numLvls
  when (retained >= cap) $ kllCompress sk

kllCompress :: PrimMonad m => KllSketch (PrimState m) -> m ()
kllCompress sk = do
  numLvls <- getNumLevels sk
  let k = kllK sk
  compressLoop 0 numLvls k
  where
    compressLoop !h !numLvls !k
      | h >= numLvls = pure ()
      | otherwise = do
          lvls <- readMutVar (kllLevels sk)
          let lvl = lvls Vector.! h
          cnt <- levelCount lvl
          let cap = levelCapacity k numLvls h
          if cnt >= cap && cnt >= 2
            then do
              currentNumLvls <- getNumLevels sk
              when (h + 1 >= currentNumLvls) $ addLevel sk
              updatedNumLvls <- getNumLevels sk
              compactLevel sk h
              compressLoop (h + 1) updatedNumLvls k
            else compressLoop (h + 1) numLvls k

addLevel :: PrimMonad m => KllSketch (PrimState m) -> m ()
addLevel sk = do
  numLvls <- getNumLevels sk
  let cap = levelCapacity (kllK sk) (numLvls + 1) numLvls
  newLvl <- mkLevel cap
  modifyMutVar' (kllLevels sk) (`Vector.snoc` newLvl)

compactLevel :: PrimMonad m => KllSketch (PrimState m) -> Int -> m ()
compactLevel sk h = do
  lvls <- readMutVar (kllLevels sk)
  let srcLvl = lvls Vector.! h
      dstLvl = lvls Vector.! (h + 1)
  cnt <- levelCount srcLvl
  buf <- levelBuffer srcLvl

  sortByBoundsM buf 0 cnt

  -- Randomly choose evens or odds to promote; discard the rest
  coin <- uniform (kllRng sk)
  let startIdx = if coin then 1 else 0

  promoteLoop buf startIdx cnt dstLvl

  -- Compacted items are discarded from this level
  levelClear srcLvl
  where
    sortByBoundsM v lo hi = do
      let slice = MUVector.slice lo (hi - lo) v
      sort slice

    promoteLoop buf !i !n dstLvl
      | i >= n = pure ()
      | otherwise = do
          val <- MUVector.unsafeRead buf i
          levelAppend dstLvl val
          promoteLoop buf (i + 2) n dstLvl

-- | Get all weighted items from the sketch for quantile computation.
-- Returns (value, weight) pairs sorted by value.
getWeightedItems :: PrimMonad m => KllSketch (PrimState m) -> m (UVector.Vector (Double, Word64))
getWeightedItems sk = do
  lvls <- readMutVar (kllLevels sk)
  totalRetained <- kllRetainedItems sk
  items <- MUVector.new totalRetained
  let fillLevel !writeIdx !h lvl = do
        cnt <- levelCount lvl
        buf <- levelBuffer lvl
        let weight = (1 :: Word64) `shiftL` h
            go !i !w
              | i >= cnt = pure w
              | otherwise = do
                  val <- MUVector.unsafeRead buf i
                  MUVector.unsafeWrite items w (val, weight)
                  go (i + 1) (w + 1)
        go 0 writeIdx
  finalIdx <- Vector.ifoldM fillLevel 0 lvls
  frozen <- UVector.unsafeFreeze (MUVector.slice 0 finalIdx items)
  let sorted = UVector.modify sort frozen
  pure sorted

-- | Get the approximate quantile value for a given normalized rank [0, 1].
kllQuantile :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
kllQuantile sk normRank = do
  empty <- kllIsEmpty sk
  if empty
    then pure (0/0)
    else do
      when (normRank < 0 || normRank > 1.0) $
        error $ "KLL: Normalized rank must be in [0.0, 1.0]: " ++ show normRank
      totalN <- kllCount sk
      items <- getWeightedItems sk
      let targetWeight = floor (normRank * fromIntegral totalN) :: Word64
      pure (findQuantile items targetWeight totalN)

findQuantile :: UVector.Vector (Double, Word64) -> Word64 -> Word64 -> Double
findQuantile items targetWeight totalN
  | UVector.null items = 0/0
  | otherwise =
      let cumWeights = UVector.postscanl' (\acc (_, w) -> acc + w) 0 items
          go !i
            | i >= UVector.length cumWeights = fst (UVector.last items)
            | cumWeights UVector.! i > targetWeight = fst (items UVector.! i)
            | otherwise = go (i + 1)
      in go 0

-- | Get the approximate normalized rank of a value.
kllRank :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
kllRank sk value = do
  empty <- kllIsEmpty sk
  if empty
    then pure (0/0)
    else do
      totalN <- kllCount sk
      items <- getWeightedItems sk
      let countBelow = UVector.foldl'
            (\acc (v, w) -> if v < value then acc + w else acc)
            0
            items
      pure (fromIntegral countBelow / fromIntegral totalN)

-- | Merge the second sketch into the first.
kllMerge :: PrimMonad m => KllSketch (PrimState m) -> KllSketch (PrimState m) -> m ()
kllMerge this other = do
  otherEmpty <- kllIsEmpty other
  unless otherEmpty $ do
    otherN <- kllCount other
    modifyURef (kllTotalN this) (+ otherN)

    thisMin <- readURef (kllMinValue this)
    otherMin <- readURef (kllMinValue other)
    when (isNaN thisMin || otherMin < thisMin) $
      writeURef (kllMinValue this) otherMin

    thisMax <- readURef (kllMaxValue this)
    otherMax <- readURef (kllMaxValue other)
    when (isNaN thisMax || otherMax > thisMax) $
      writeURef (kllMaxValue this) otherMax

    otherLvls <- readMutVar (kllLevels other)
    Vector.iforM_ otherLvls $ \h otherLvl -> do
      otherCnt <- levelCount otherLvl
      when (otherCnt > 0) $ do
        thisNumLvls <- getNumLevels this
        growUntil this (h + 1)
        thisLvls <- readMutVar (kllLevels this)
        let thisLvl = thisLvls Vector.! h
        otherBuf <- levelBuffer otherLvl
        forM_ [0..otherCnt - 1] $ \i -> do
          val <- MUVector.unsafeRead otherBuf i
          levelAppend thisLvl val

    compressIfNeeded this
  where
    growUntil sk target = do
      n <- getNumLevels sk
      when (n < target) $ do
        addLevel sk
        growUntil sk target
