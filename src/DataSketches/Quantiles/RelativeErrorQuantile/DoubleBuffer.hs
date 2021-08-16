{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
module DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer
  ( DoubleBuffer
  , append
  , ensureCapacity
  , getCountWithCriterion
  , getEvensOrOdds
  , (!) -- getItem
  , delta
  , spaceAtBottom
  , getCount
  , getSpace
  , isEmpty
  , isSorted
  , sort
  , mergeSortIn
  , trimCapacity
  , trimCount
  ) where

import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Control.Monad
import Control.Monad.Primitive
import qualified Data.Vector.Unboxed.Mutable as MUVector
import DataSketches.Quantiles.RelativeErrorQuantile.URef
import Data.Vector.Algorithms.Intro (sortByBounds)
import Data.Vector.Algorithms.Search (binarySearchPBounds)

data DoubleBuffer s = DoubleBuffer
  { vec :: !(MUVector.MVector s Double)
  , count :: !(URef s Int)
  , capacity :: !(URef s Int)
  , sorted :: !(URef s Bool)
  , delta :: !Int
  , spaceAtBottom :: !Bool
  }

append :: PrimMonad m => DoubleBuffer (PrimState m) -> Double -> m ()
append buf@DoubleBuffer{..} x = do
  ensureSpace buf 1
  index <- if spaceAtBottom
    then
      (\capacity_ count_ -> capacity_ - count_ - 1)
        <$> readURef capacity
        <*> readURef count
    else readURef count
  MUVector.write vec index x
  writeURef sorted False

ensureSpace :: PrimMonad m => DoubleBuffer (PrimState m) -> Int -> m ()
ensureSpace buf@DoubleBuffer{..} space = do
  count_ <- readURef count
  capacity_ <- readURef capacity
  let notEnoughSpace = count_ + space > capacity_
  when notEnoughSpace $ do
    let newCap = count_ + space + delta
    ensureCapacity buf newCap

ensureCapacity :: PrimMonad m => DoubleBuffer (PrimState m) -> Int -> m ()
ensureCapacity DoubleBuffer{..} newCapacity = do
  capacity_ <- readURef capacity
  when (newCapacity > capacity_) $ do
    error "Foo"

getCountWithCriterion :: PrimMonad m => DoubleBuffer (PrimState m) -> Double -> Criterion -> m Int
getCountWithCriterion buf@DoubleBuffer{..} value criterion = do
  sorted_ <- readURef sorted
  unless sorted_ $ sort buf
  (low, high) <- if spaceAtBottom
    then do
      capacity_ <- readURef capacity
      count_ <- readURef count
      pure (capacity_ - count_, capacity_ - 1)
    else do
      high <- (\x -> x - 1) <$> readURef count
      pure (0, high)
  
  ix <- binarySearchPBounds pred vec low high
  pure $! if ix == MUVector.length vec 
    then 0
    else ix - low + 1 
  where
    pred = case criterion of
      (:<) -> (< value)
      (:<=) -> (<= value)
  
data EvensOrOdds = Evens | Odds

getEvensOrOdds :: PrimMonad m => DoubleBuffer (PrimState m) -> Int -> Int -> EvensOrOdds -> m (DoubleBuffer (PrimState m))
getEvensOrOdds = undefined

(!) :: PrimMonad m => DoubleBuffer (PrimState m) -> Int -> m Double
(!) DoubleBuffer{..} offset = do
  index <- if spaceAtBottom
    then do
      capacity_ <- readURef capacity
      count_ <- readURef count
      pure $! capacity_ - count_ + offset
    else pure offset
  MUVector.read vec index
  
getCount :: PrimMonad m => DoubleBuffer (PrimState m) -> m Int
getCount = readURef . count

getSpace :: PrimMonad m => DoubleBuffer (PrimState m) -> m Int
getSpace DoubleBuffer{..} = (-) <$> readURef capacity <*> readURef count

isEmpty :: PrimMonad m => DoubleBuffer (PrimState m) -> m Bool
isEmpty buf = (== 0) <$> getCount buf

isSorted :: PrimMonad m => DoubleBuffer (PrimState m) -> m Bool
isSorted = readURef . sorted

sort :: PrimMonad m => DoubleBuffer (PrimState m) -> m ()
sort buf@DoubleBuffer{..} = do
  sorted_ <- isSorted buf
  unless sorted_ $ do
    capacity_ <- readURef capacity 
    count_ <- readURef count
    let (start, end) = if spaceAtBottom
          then (capacity_ - count_, capacity_)
          else (0, count_)
    sortByBounds compare vec start end
    writeURef sorted True

mergeSortIn :: PrimMonad m => DoubleBuffer (PrimState m) -> DoubleBuffer (PrimState m) -> m (DoubleBuffer (PrimState m))
mergeSortIn this bufIn = do
  sort this
  sort bufIn
  let arrIn = vec bufIn
  bufInLen <- getCount bufIn
  ensureSpace this bufInLen
  totalLength <- (+ bufInLen) <$> getCount this
  modifyURef (count this) (+ bufInLen)
  count_ <- readURef (count this)
  if spaceAtBottom this
    then do -- scan up, insert at bottom
      capacity_ <- readURef (capacity this)
      bufInCapacity_ <- readURef (capacity bufIn)
      let i = capacity_ - count_
      let j = bufInCapacity_ - bufInLen
      let targetStart = capacity_ - totalLength
      let k = targetStart
      mergeUpwards capacity_ bufInCapacity_ i j k
    else do -- scan down, insert at top
      let i = count_ - 1
      let j = bufInLen - 1
      let k = totalLength
      mergeDownwards i j k
  pure this
  where
    mergeUpwards capacity_ bufInCapacity_ = go
      where 
        go !i !j !k
          -- for loop ended
          | k >= capacity_ = pure ()
          -- both valid
          | i < capacity_ && j < bufInCapacity_ = do
            iVal <- MUVector.read (vec this) i
            jVal <- MUVector.read (vec bufIn) j
            if iVal <= jVal
              then MUVector.write (vec this) k iVal >> go (i + 1) j (k + 1)
              else MUVector.write (vec this) k jVal >> go i (j + 1) (k + 1)
          -- i is valid
          | i < capacity_ = do
            MUVector.write (vec this) k =<< MUVector.unsafeRead (vec this) i
            go (i + 1) j (k + 1)
          -- j is valid
          | j < bufInCapacity_ = do
            MUVector.write (vec this) k =<< MUVector.unsafeRead (vec this) j
            go i (j + 1) (k + 1)
          -- neither is valid, break;
          | otherwise = pure ()
    mergeDownwards !i !j !k
      -- for loop ended
      | k < 0 = pure ()
      -- both valid
      | i >= 0 && j >= 0 = do
        iVal <- MUVector.read (vec this) i
        jVal <- MUVector.read (vec bufIn) j
        if iVal >= jVal
          then MUVector.write (vec this) k iVal >> mergeDownwards (i - 1) j (k - 1)
          else MUVector.write (vec this) k jVal >> mergeDownwards i (j - 1) (k - 1)
      | i >= 0 = do
        MUVector.write (vec this) k =<< MUVector.unsafeRead (vec this) i
        mergeDownwards (i - 1) j (k - 1)
      | k >= 0 = do
        MUVector.write (vec this) k =<< MUVector.unsafeRead (vec this) j
        mergeDownwards i (j - 1) (k - 1)
      -- neither is valid, break;
      | otherwise = pure ()

trimCapacity :: PrimMonad m => DoubleBuffer (PrimState m) -> m ()
trimCapacity DoubleBuffer{..} = error "Sad"

trimCount :: PrimMonad m => DoubleBuffer (PrimState m) -> Int -> m ()
trimCount DoubleBuffer{..} newCount = modifyURef count (\oldCount -> if newCount < oldCount then newCount else oldCount)