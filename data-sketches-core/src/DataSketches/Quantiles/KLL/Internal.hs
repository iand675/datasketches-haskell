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
import Data.Bits (shiftL)
import Data.Primitive.MutVar
import Data.Word
import qualified Data.Vector.Unboxed as UVector
import qualified Data.Vector.Unboxed.Mutable as MUVector
import Data.Vector.Algorithms.Intro (sort)
import System.Random.MWC (Gen, uniform, create)
import DataSketches.Core.Internal.URef

minLevelSize :: Int
minLevelSize = 2

-- | KLL sketch using flat contiguous unboxed storage.
--
-- All item data is in a single unboxed mutable vector of Doubles (ByteArray#
-- under the hood — same representation as Java's @double[]@). Level boundaries
-- are tracked in a separate unboxed Int vector.
--
-- Level 0 grows leftward (prepend) so that insert is O(1) — no shifting.
-- Higher levels occupy the right side of the array and are stable.
--
-- @
-- items: [ free | level 0 items | level 1 items | level 2 items | ... ]
--                 ^               ^               ^               ^
--                 levels[0]       levels[1]       levels[2]       levels[numLevels]
-- @
data KllSketch s = KllSketch
  { kllK :: !Word32
  , kllTotalN :: {-# UNPACK #-} !(URef s Word64)
  , kllMinValue :: {-# UNPACK #-} !(URef s Double)
  , kllMaxValue :: {-# UNPACK #-} !(URef s Double)
  , kllNumLevels :: {-# UNPACK #-} !(URef s Int)
  , kllItems :: {-# UNPACK #-} !(MutVar s (MUVector.MVector s Double))
  , kllLevels :: {-# UNPACK #-} !(MutVar s (MUVector.MVector s Int))
  , kllRng :: {-# UNPACK #-} !(Gen s)
  }

instance NFData (KllSketch s) where rnf !_ = ()

levelCapacity :: Word32 -> Int -> Int -> Int
levelCapacity k numLevels h = max minLevelSize cap
  where
    depth = numLevels - 1 - h
    cap = round (fromIntegral k * (2.0/3.0 :: Double) ^^ depth)
{-# INLINE levelCapacity #-}

totalCapacity :: Word32 -> Int -> Int
totalCapacity k numLevels = go 0 0
  where
    go !acc !h
      | h >= numLevels = acc
      | otherwise = go (acc + levelCapacity k numLevels h) (h + 1)

mkKllSketch :: PrimMonad m => Word32 -> m (KllSketch (PrimState m))
mkKllSketch k = do
  unless (k >= 8) $ error "KLL sketch: k must be >= 8"
  let initCap = fromIntegral k * 4
  items <- MUVector.new initCap
  -- levels[0] = initCap (level 0 starts at end, grows left)
  -- levels[1] = initCap (level 0 is empty)
  lvls <- MUVector.new 8
  MUVector.unsafeWrite lvls 0 initCap
  MUVector.unsafeWrite lvls 1 initCap
  KllSketch k
    <$> newURef 0
    <*> newURef (0/0)
    <*> newURef (0/0)
    <*> newURef 1
    <*> newMutVar items
    <*> newMutVar lvls
    <*> create

kllIsEmpty :: PrimMonad m => KllSketch (PrimState m) -> m Bool
kllIsEmpty sk = (== 0) <$> readURef (kllTotalN sk)
{-# INLINE kllIsEmpty #-}

kllCount :: PrimMonad m => KllSketch (PrimState m) -> m Word64
kllCount = readURef . kllTotalN
{-# INLINE kllCount #-}

kllMinimum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMinimum = readURef . kllMinValue
{-# INLINE kllMinimum #-}

kllMaximum :: PrimMonad m => KllSketch (PrimState m) -> m Double
kllMaximum = readURef . kllMaxValue
{-# INLINE kllMaximum #-}

getNumLevels :: PrimMonad m => KllSketch (PrimState m) -> m Int
getNumLevels = readURef . kllNumLevels
{-# INLINE getNumLevels #-}

kllRetainedItems :: PrimMonad m => KllSketch (PrimState m) -> m Int
kllRetainedItems sk = do
  lvls <- readMutVar (kllLevels sk)
  numLvls <- getNumLevels sk
  lo <- MUVector.unsafeRead lvls 0
  hi <- MUVector.unsafeRead lvls numLvls
  pure $! hi - lo
{-# INLINE kllRetainedItems #-}

levelSize :: PrimMonad m => KllSketch (PrimState m) -> Int -> m Int
levelSize sk h = do
  lvls <- readMutVar (kllLevels sk)
  lo <- MUVector.unsafeRead lvls h
  hi <- MUVector.unsafeRead lvls (h + 1)
  pure $! hi - lo
{-# INLINE levelSize #-}

-- | Insert a value. Level 0 grows leftward (O(1) prepend).
kllInsert :: PrimMonad m => KllSketch (PrimState m) -> Double -> m ()
kllInsert sk !val = do
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
    lvl0Start <- MUVector.unsafeRead lvls 0
    items <- readMutVar (kllItems sk)

    if lvl0Start > 0
      then do
        let !newStart = lvl0Start - 1
        MUVector.unsafeWrite items newStart val
        MUVector.unsafeWrite lvls 0 newStart
      else do
        -- No free space at the beginning; grow the array
        let oldCap = MUVector.length items
            growBy = max oldCap (fromIntegral (kllK sk))
            newCap = oldCap + growBy
        newItems <- MUVector.new newCap
        numLvls <- getNumLevels sk
        endAll <- MUVector.unsafeRead lvls numLvls
        let usedLen = endAll -- items are at [0, endAll)
        -- Copy old items to the end of new array, leaving growBy free at start
        MUVector.copy (MUVector.slice growBy usedLen newItems) (MUVector.slice 0 usedLen items)
        writeMutVar (kllItems sk) newItems
        -- Shift all level boundaries by growBy
        forM_ [0 .. numLvls] $ \i ->
          MUVector.unsafeModify lvls (+ growBy) i
        -- Now prepend the new item
        newLvl0 <- MUVector.unsafeRead lvls 0
        let !newStart = newLvl0 - 1
        MUVector.unsafeWrite newItems newStart val
        MUVector.unsafeWrite lvls 0 newStart

    modifyURef (kllTotalN sk) (+ 1)
    compressIfNeeded sk
{-# INLINE kllInsert #-}

compressIfNeeded :: PrimMonad m => KllSketch (PrimState m) -> m ()
compressIfNeeded sk = do
  retained <- kllRetainedItems sk
  numLvls <- getNumLevels sk
  let cap = totalCapacity (kllK sk) numLvls
  when (retained >= cap) $ kllCompress sk

kllCompress :: PrimMonad m => KllSketch (PrimState m) -> m ()
kllCompress sk = compressLoop 0
  where
    k = kllK sk
    compressLoop !h = do
      numLvls <- getNumLevels sk
      when (h < numLvls) $ do
        sz <- levelSize sk h
        let cap = levelCapacity k numLvls h
        if sz >= cap && sz >= 2
          then do
            when (h + 1 >= numLvls) $ addLevel sk
            compactLevel sk h
            compressLoop (h + 1)
          else compressLoop (h + 1)

addLevel :: PrimMonad m => KllSketch (PrimState m) -> m ()
addLevel sk = do
  numLvls <- getNumLevels sk
  let newNumLvls = numLvls + 1
  lvls <- readMutVar (kllLevels sk)
  let lvlsCap = MUVector.length lvls
  lvls' <- if newNumLvls + 1 > lvlsCap
    then do
      new <- MUVector.grow lvls lvlsCap
      writeMutVar (kllLevels sk) new
      pure new
    else pure lvls
  end <- MUVector.unsafeRead lvls' numLvls
  MUVector.unsafeWrite lvls' newNumLvls end
  writeURef (kllNumLevels sk) newNumLvls

-- | Compact level h: sort, randomly promote half to level h+1, discard rest.
-- In the flat layout, level h items are at [levels[h], levels[h+1]).
-- After compaction: level h shrinks, level h+1 grows by numPromoted.
compactLevel :: PrimMonad m => KllSketch (PrimState m) -> Int -> m ()
compactLevel sk h = do
  lvls <- readMutVar (kllLevels sk)
  items <- readMutVar (kllItems sk)
  lo <- MUVector.unsafeRead lvls h
  hi <- MUVector.unsafeRead lvls (h + 1)
  let !sz = hi - lo

  -- Sort level h in place
  sort (MUVector.slice lo sz items)

  -- Randomly pick evens or odds to promote
  coin <- uniform (kllRng sk)
  let !startIdx = if coin then 1 else 0
      !numPromoted = (sz - startIdx + 1) `div` 2
      !numDiscarded = sz - numPromoted

  -- Write promoted items contiguously at [lo, lo + numPromoted)
  let writePromoted !srcOff !dstOff
        | srcOff >= sz = pure ()
        | otherwise = do
            v <- MUVector.unsafeRead items (lo + srcOff)
            MUVector.unsafeWrite items (lo + dstOff) v
            writePromoted (srcOff + 2) (dstOff + 1)
  writePromoted startIdx 0

  -- Now items[lo .. lo+numPromoted-1] = promoted items.
  -- items[lo+numPromoted .. hi-1] = garbage (old data).
  -- We need to remove the discarded region and let the promoted items
  -- become part of level h+1.

  -- Shift all items after level h left by numDiscarded to close the gap.
  numLvls <- getNumLevels sk
  endAll <- MUVector.unsafeRead lvls numLvls
  let srcStart = hi
      dstStart = lo + numPromoted
      moveLen = endAll - hi
  when (moveLen > 0 && srcStart /= dstStart) $
    MUVector.move
      (MUVector.slice dstStart moveLen items)
      (MUVector.slice srcStart moveLen items)

  -- Update level boundaries.
  -- Level h is now empty: levels[h+1] = levels[h]
  -- Promoted items join level h+1: levels[h+1] stays at lo (= old levels[h])
  -- Everything after shifts left by numDiscarded.
  MUVector.unsafeWrite lvls (h + 1) lo
  forM_ [h + 2 .. numLvls] $ \i ->
    MUVector.unsafeModify lvls (subtract numDiscarded) i

getWeightedItems :: PrimMonad m => KllSketch (PrimState m) -> m (UVector.Vector (Double, Word64))
getWeightedItems sk = do
  numLvls <- getNumLevels sk
  retained <- kllRetainedItems sk
  result <- MUVector.new retained
  items <- readMutVar (kllItems sk)
  lvls <- readMutVar (kllLevels sk)
  let fillLevel !writeIdx !h
        | h >= numLvls = pure writeIdx
        | otherwise = do
            lo <- MUVector.unsafeRead lvls h
            hi <- MUVector.unsafeRead lvls (h + 1)
            let !weight = (1 :: Word64) `shiftL` h
            let go !i !w
                  | i >= hi = pure w
                  | otherwise = do
                      val <- MUVector.unsafeRead items i
                      MUVector.unsafeWrite result w (val, weight)
                      go (i + 1) (w + 1)
            newIdx <- go lo writeIdx
            fillLevel newIdx (h + 1)
  finalIdx <- fillLevel 0 0
  frozen <- UVector.unsafeFreeze (MUVector.slice 0 finalIdx result)
  pure $ UVector.modify sort frozen

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
      pure (findQuantile items targetWeight)

findQuantile :: UVector.Vector (Double, Word64) -> Word64 -> Double
findQuantile items targetWeight
  | UVector.null items = 0/0
  | otherwise =
      let cumWeights = UVector.postscanl' (\acc (_, w) -> acc + w) 0 items
          go !i
            | i >= UVector.length cumWeights = fst (UVector.last items)
            | cumWeights UVector.! i > targetWeight = fst (items UVector.! i)
            | otherwise = go (i + 1)
      in go 0

kllRank :: PrimMonad m => KllSketch (PrimState m) -> Double -> m Double
kllRank sk value = do
  empty <- kllIsEmpty sk
  if empty
    then pure (0/0)
    else do
      totalN <- kllCount sk
      items <- getWeightedItems sk
      let !countBelow = UVector.foldl'
            (\acc (v, w) -> if v < value then acc + w else acc)
            0
            items
      pure (fromIntegral countBelow / fromIntegral totalN)

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

    -- Insert all retained items from other into this
    otherNumLvls <- getNumLevels other
    otherItems <- readMutVar (kllItems other)
    otherLvls <- readMutVar (kllLevels other)

    forM_ [0 .. otherNumLvls - 1] $ \h -> do
      lo <- MUVector.unsafeRead otherLvls h
      hi <- MUVector.unsafeRead otherLvls (h + 1)
      forM_ [lo .. hi - 1] $ \i -> do
        val <- MUVector.unsafeRead otherItems i
        kllInsertAtLevel this h val

    compressIfNeeded this

-- Insert a value at a specific level. For level 0, prepend. For higher levels, append.
kllInsertAtLevel :: PrimMonad m => KllSketch (PrimState m) -> Int -> Double -> m ()
kllInsertAtLevel sk 0 val = do
  -- Same as regular insert without min/max/count tracking
  lvls <- readMutVar (kllLevels sk)
  lvl0Start <- MUVector.unsafeRead lvls 0
  items <- readMutVar (kllItems sk)
  if lvl0Start > 0
    then do
      let !newStart = lvl0Start - 1
      MUVector.unsafeWrite items newStart val
      MUVector.unsafeWrite lvls 0 newStart
    else do
      growAndShift sk
      kllInsertAtLevel sk 0 val
kllInsertAtLevel sk h val = do
  -- For higher levels, append at the end of level h
  -- This requires shifting levels h+1.. right by 1
  lvls <- readMutVar (kllLevels sk)
  numLvls <- getNumLevels sk
  endAll <- MUVector.unsafeRead lvls numLvls
  items <- readMutVar (kllItems sk)
  let cap = MUVector.length items
  items' <- if endAll >= cap
    then do
      let newCap = max (cap * 2) (endAll + 1)
      new <- MUVector.grow items (newCap - cap)
      writeMutVar (kllItems sk) new
      pure new
    else pure items
  -- Shift items after level h right by 1
  hiH <- MUVector.unsafeRead lvls (h + 1)
  let moveLen = endAll - hiH
  when (moveLen > 0) $
    MUVector.move
      (MUVector.slice (hiH + 1) moveLen items')
      (MUVector.slice hiH moveLen items')
  MUVector.unsafeWrite items' hiH val
  -- Update boundaries
  forM_ [h + 1 .. numLvls] $ \i ->
    MUVector.unsafeModify lvls (+ 1) i

growAndShift :: PrimMonad m => KllSketch (PrimState m) -> m ()
growAndShift sk = do
  items <- readMutVar (kllItems sk)
  lvls <- readMutVar (kllLevels sk)
  numLvls <- getNumLevels sk
  let oldCap = MUVector.length items
      growBy = max oldCap (fromIntegral (kllK sk))
      newCap = oldCap + growBy
  newItems <- MUVector.new newCap
  endAll <- MUVector.unsafeRead lvls numLvls
  lo0 <- MUVector.unsafeRead lvls 0
  let usedLen = endAll - lo0
  MUVector.copy (MUVector.slice (lo0 + growBy) usedLen newItems) (MUVector.slice lo0 usedLen items)
  writeMutVar (kllItems sk) newItems
  forM_ [0 .. numLvls] $ \i ->
    MUVector.unsafeModify lvls (+ growBy) i
