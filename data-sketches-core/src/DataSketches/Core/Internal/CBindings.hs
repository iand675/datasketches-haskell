{-# LANGUAGE ForeignFunctionInterface #-}
module DataSketches.Core.Internal.CBindings
  ( -- * HyperLogLog
    c_hll_insert
  , c_hll_estimate
    -- * Count-Min Sketch
  , c_cms_insert
  , c_cms_estimate
    -- * xoshiro256++ RNG
  , c_xoshiro_next
  , c_xoshiro_coin
  , c_xoshiro_seed
    -- * Sort
  , c_sort_doubles
  ) where

import Data.Word
import Foreign.Ptr
import Foreign.C.Types

foreign import ccall unsafe "hll_insert"
  c_hll_insert :: Ptr Word8 -> CInt -> Word64 -> IO ()

foreign import ccall unsafe "hll_estimate"
  c_hll_estimate :: Ptr Word8 -> CInt -> Ptr CDouble -> Ptr CDouble -> Ptr CInt -> IO ()

foreign import ccall unsafe "cms_insert"
  c_cms_insert :: Ptr Word64 -> Ptr Word64 -> CInt -> CInt -> Word64 -> Word64 -> IO ()

foreign import ccall unsafe "cms_estimate"
  c_cms_estimate :: Ptr Word64 -> Ptr Word64 -> CInt -> CInt -> Word64 -> IO Word64

foreign import ccall unsafe "xoshiro256pp_next"
  c_xoshiro_next :: Ptr Word64 -> IO Word64

foreign import ccall unsafe "xoshiro256pp_coin"
  c_xoshiro_coin :: Ptr Word64 -> IO CInt

foreign import ccall unsafe "xoshiro256pp_seed"
  c_xoshiro_seed :: Ptr Word64 -> Word64 -> IO ()

foreign import ccall unsafe "sort_doubles"
  c_sort_doubles :: Ptr CDouble -> CInt -> IO ()
