{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
module DataSketches.Core.Internal.URef
  ( URef
  , IOURef
  , newURef
  , readURef
  , writeURef
  , modifyURef
  -- * Packed mutable byte arrays for multiple fields
  , MutableFields
  , newMutableFields
  , readField
  , writeField
  , modifyField
  ) where

import Control.Monad.Primitive
import Data.Primitive.ByteArray
import qualified Data.Vector.Unboxed.Mutable as MUVector
import Data.Vector.Unboxed (Unbox)
import Data.Primitive (Prim, readByteArray, writeByteArray, sizeOf)

-- | An unboxed reference. Stores a single value in a 'MutableByteArray'.
newtype URef s a = URef (MUVector.MVector s a)

type IOURef = URef (PrimState IO)

newURef :: (PrimMonad m, Unbox a) => a -> m (URef (PrimState m) a)
newURef a = fmap URef (MUVector.replicate 1 a)
{-# INLINE newURef #-}

readURef :: (PrimMonad m, Unbox a) => URef (PrimState m) a -> m a
readURef (URef v) = MUVector.unsafeRead v 0
{-# INLINE readURef #-}

writeURef :: (PrimMonad m, Unbox a) => URef (PrimState m) a -> a -> m ()
writeURef (URef v) = MUVector.unsafeWrite v 0
{-# INLINE writeURef #-}

modifyURef :: (PrimMonad m, Unbox a) => URef (PrimState m) a -> (a -> a) -> m ()
modifyURef (URef v) f = do
  !x <- MUVector.unsafeRead v 0
  MUVector.unsafeWrite v 0 $! f x
{-# INLINE modifyURef #-}

-- | A single 'MutableByteArray' that packs multiple typed fields at byte offsets.
-- Use 'readField' and 'writeField' with the byte offset of each field.
-- Callers are responsible for computing non-overlapping offsets from 'Data.Primitive.sizeOf'.
newtype MutableFields s = MutableFields (MutableByteArray s)

-- | Allocate a packed mutable fields block of the given total size in bytes.
newMutableFields :: PrimMonad m => Int -> m (MutableFields (PrimState m))
newMutableFields size = MutableFields <$> newByteArray size
{-# INLINE newMutableFields #-}

-- | Read a 'Prim' value at the given ELEMENT index (not byte offset).
-- The index is in units of @sizeOf a@.
readField :: (PrimMonad m, Prim a) => MutableFields (PrimState m) -> Int -> m a
readField (MutableFields mba) = readByteArray mba
{-# INLINE readField #-}

-- | Write a 'Prim' value at the given ELEMENT index.
writeField :: (PrimMonad m, Prim a) => MutableFields (PrimState m) -> Int -> a -> m ()
writeField (MutableFields mba) = writeByteArray mba
{-# INLINE writeField #-}

-- | Strict read-modify-write at the given ELEMENT index.
modifyField :: (PrimMonad m, Prim a) => MutableFields (PrimState m) -> Int -> (a -> a) -> m ()
modifyField mf ix f = do
  !x <- readField mf ix
  writeField mf ix $! f x
{-# INLINE modifyField #-}
