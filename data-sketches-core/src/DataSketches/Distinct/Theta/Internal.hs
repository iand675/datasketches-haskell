{-# LANGUAGE MagicHash #-}
module DataSketches.Distinct.Theta.Internal
  ( ThetaSketch(..)
  , mkThetaSketch
  , thetaInsert
  , thetaEstimate
  , thetaUnion
  , thetaIntersection
  , thetaDifference
  , thetaIsEmpty
  ) where

import Control.Monad (when, unless, forM_)
import Control.Monad.Primitive
import Data.Bits
import Data.Word
import Data.Primitive.MutVar
import qualified Data.Vector.Unboxed as UVector
import qualified Data.Vector.Unboxed.Mutable as MUVector
import qualified Data.Vector.Algorithms.Intro as Sort
import DataSketches.Core.Internal.URef

-- | Theta Sketch for distinct counting with set operations.
--
-- Maintains a set of hash values below a threshold theta.
-- theta starts at maxBound and decreases as more items are inserted,
-- maintaining at most k retained entries.
--
-- The estimate is: (number of retained entries) / theta
--
-- Set operations (union, intersection, difference) work by combining
-- hash sets and adjusting theta.
data ThetaSketch s = ThetaSketch
  { tsK :: {-# UNPACK #-} !Int
  , tsTheta :: {-# UNPACK #-} !(URef s Word64)
  , tsEntries :: {-# UNPACK #-} !(MutVar s (MUVector.MVector s Word64))
  , tsCount :: {-# UNPACK #-} !(URef s Int)
  , tsEmpty :: {-# UNPACK #-} !(URef s Bool)
  }

maxTheta :: Word64
maxTheta = maxBound

-- Murmur3-style 64-bit finalizer
murmurHash64 :: Word64 -> Word64
murmurHash64 h0 =
  let h1 = (h0 `xor` (h0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
      h2 = (h1 `xor` (h1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
  in h2 `xor` (h2 `shiftR` 33)

-- | Create a new Theta sketch.
-- k determines the maximum number of retained entries.
-- Larger k gives better accuracy but uses more space.
-- k must be a power of 2 and >= 16.
-- Standard error is approximately 1/sqrt(k).
mkThetaSketch :: PrimMonad m => Int -> m (ThetaSketch (PrimState m))
mkThetaSketch k = do
  when (k < 16) $ error "Theta: k must be >= 16"
  entries <- MUVector.new (k * 2)
  ThetaSketch k
    <$> newURef maxTheta
    <*> newMutVar entries
    <*> newURef 0
    <*> newURef True

thetaIsEmpty :: PrimMonad m => ThetaSketch (PrimState m) -> m Bool
thetaIsEmpty = readURef . tsEmpty

-- | Insert a hashed item into the sketch.
thetaInsert :: PrimMonad m => ThetaSketch (PrimState m) -> Word64 -> m ()
thetaInsert sk item = do
  let hash = murmurHash64 item
  when (hash == 0) $ pure ()
  theta <- readURef (tsTheta sk)
  when (hash < theta) $ do
    writeURef (tsEmpty sk) False
    cnt <- readURef (tsCount sk)
    entries <- readMutVar (tsEntries sk)
    let cap = MUVector.length entries

    -- Check for duplicate
    isDup <- checkDuplicate entries cnt hash
    unless isDup $ do
      if cnt >= cap
        then do
          newEntries <- MUVector.grow entries cap
          writeMutVar (tsEntries sk) newEntries
          MUVector.unsafeWrite newEntries cnt hash
          writeURef (tsCount sk) (cnt + 1)
          rebuildIfNeeded sk
        else do
          MUVector.unsafeWrite entries cnt hash
          writeURef (tsCount sk) (cnt + 1)
          rebuildIfNeeded sk

checkDuplicate :: PrimMonad m => MUVector.MVector (PrimState m) Word64 -> Int -> Word64 -> m Bool
checkDuplicate entries cnt hash = go 0
  where
    go !i
      | i >= cnt = pure False
      | otherwise = do
          val <- MUVector.unsafeRead entries i
          if val == hash then pure True else go (i + 1)

rebuildIfNeeded :: PrimMonad m => ThetaSketch (PrimState m) -> m ()
rebuildIfNeeded sk = do
  cnt <- readURef (tsCount sk)
  when (cnt > tsK sk) $ rebuild sk

rebuild :: PrimMonad m => ThetaSketch (PrimState m) -> m ()
rebuild sk = do
  cnt <- readURef (tsCount sk)
  entries <- readMutVar (tsEntries sk)
  let k = tsK sk

  -- Sort the entries
  Sort.sortByBounds compare entries 0 cnt

  -- The k-th smallest hash becomes the new theta
  let newTheta_idx = k - 1
  newTheta <- MUVector.unsafeRead entries newTheta_idx

  -- Keep only entries < newTheta (the first k-1 or k entries)
  -- Find how many entries are strictly less than newTheta
  let countBelow !i
        | i >= cnt = pure i
        | otherwise = do
            val <- MUVector.unsafeRead entries i
            if val < newTheta then countBelow (i + 1) else pure i
  belowCount <- countBelow 0

  writeURef (tsTheta sk) newTheta
  writeURef (tsCount sk) belowCount

-- | Estimate the number of distinct items.
thetaEstimate :: PrimMonad m => ThetaSketch (PrimState m) -> m Double
thetaEstimate sk = do
  empty <- readURef (tsEmpty sk)
  if empty
    then pure 0
    else do
      theta <- readURef (tsTheta sk)
      cnt <- readURef (tsCount sk)
      if theta == maxTheta
        then pure (fromIntegral cnt)
        else pure (fromIntegral cnt / (fromIntegral theta / fromIntegral maxTheta))

-- Helper: get all current entries as a frozen vector
getEntries :: PrimMonad m => ThetaSketch (PrimState m) -> m (UVector.Vector Word64)
getEntries sk = do
  cnt <- readURef (tsCount sk)
  entries <- readMutVar (tsEntries sk)
  UVector.freeze (MUVector.slice 0 cnt entries)

-- | Compute the union of two sketches, returning a new sketch.
thetaUnion :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaUnion a b = do
  emptyA <- thetaIsEmpty a
  emptyB <- thetaIsEmpty b
  thetaA <- readURef (tsTheta a)
  thetaB <- readURef (tsTheta b)
  let k = max (tsK a) (tsK b)
      minTheta = min thetaA thetaB
  result <- mkThetaSketch k
  writeURef (tsTheta result) minTheta

  unless emptyA $ do
    writeURef (tsEmpty result) False
    entriesA <- getEntries a
    UVector.forM_ entriesA $ \h ->
      when (h < minTheta) $ insertHash result h

  unless emptyB $ do
    writeURef (tsEmpty result) False
    entriesB <- getEntries b
    UVector.forM_ entriesB $ \h ->
      when (h < minTheta) $ insertHash result h

  rebuildIfNeeded result
  pure result

-- | Compute the intersection of two sketches, returning a new sketch.
thetaIntersection :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaIntersection a b = do
  emptyA <- thetaIsEmpty a
  emptyB <- thetaIsEmpty b
  let k = max (tsK a) (tsK b)
  result <- mkThetaSketch k

  if emptyA || emptyB
    then pure result
    else do
      thetaA <- readURef (tsTheta a)
      thetaB <- readURef (tsTheta b)
      let minTheta = min thetaA thetaB
      writeURef (tsTheta result) minTheta
      writeURef (tsEmpty result) False

      entriesA <- getEntries a
      entriesB <- getEntries b

      -- For each entry in A, check if it exists in B and is below minTheta
      UVector.forM_ entriesA $ \h ->
        when (h < minTheta && UVector.elem h entriesB) $
          insertHash result h

      rebuildIfNeeded result
      pure result

-- | Compute the set difference (A not B), returning a new sketch.
thetaDifference :: PrimMonad m => ThetaSketch (PrimState m) -> ThetaSketch (PrimState m) -> m (ThetaSketch (PrimState m))
thetaDifference a b = do
  emptyA <- thetaIsEmpty a
  let k = tsK a
  result <- mkThetaSketch k

  if emptyA
    then pure result
    else do
      thetaA <- readURef (tsTheta a)
      thetaB <- readURef (tsTheta b)
      let minTheta = min thetaA thetaB
      writeURef (tsTheta result) minTheta
      writeURef (tsEmpty result) False

      entriesA <- getEntries a
      entriesB <- getEntries b

      UVector.forM_ entriesA $ \h ->
        when (h < minTheta && not (UVector.elem h entriesB)) $
          insertHash result h

      rebuildIfNeeded result
      pure result

-- Insert a raw hash value (no re-hashing), used for set operations
insertHash :: PrimMonad m => ThetaSketch (PrimState m) -> Word64 -> m ()
insertHash sk hash = do
  theta <- readURef (tsTheta sk)
  when (hash < theta && hash /= 0) $ do
    cnt <- readURef (tsCount sk)
    entries <- readMutVar (tsEntries sk)
    isDup <- checkDuplicate entries cnt hash
    unless isDup $ do
      let cap = MUVector.length entries
      if cnt >= cap
        then do
          newEntries <- MUVector.grow entries cap
          writeMutVar (tsEntries sk) newEntries
          MUVector.unsafeWrite newEntries cnt hash
          writeURef (tsCount sk) (cnt + 1)
        else do
          MUVector.unsafeWrite entries cnt hash
          writeURef (tsCount sk) (cnt + 1)
