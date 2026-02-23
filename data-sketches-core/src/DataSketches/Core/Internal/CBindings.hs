{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Core.Internal.CBindings
  ( -- * xoshiro256++ RNG
    c_xoshiro_next
  , c_xoshiro_coin
  , c_xoshiro_seed
  ) where

import Data.Word
import Foreign.Ptr
import Foreign.C.Types

foreign import ccall unsafe "xoshiro256pp_next"
  c_xoshiro_next :: Ptr Word64 -> IO Word64

foreign import ccall unsafe "xoshiro256pp_coin"
  c_xoshiro_coin :: Ptr Word64 -> IO CInt

foreign import ccall unsafe "xoshiro256pp_seed"
  c_xoshiro_seed :: Ptr Word64 -> Word64 -> IO ()
