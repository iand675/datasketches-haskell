{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
module DataSketches.Quantiles.RelativeErrorQuantile.Compactor
  ( ReqCompactor
  , CompactorReturn
  , compact
  , getBuffer
  , getCoin
  , getLgWeight
  , getNominalCapacity
  , getNumSections
  , merge
  ) where

import GHC.TypeLits
import Data.Semigroup (Semigroup)
import Data.Word
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import System.Random.MWC (Variate(uniformR))
import Control.Monad.Trans
import Control.Monad.Primitive
import DataSketches.Quantiles.RelativeErrorQuantile.DoubleBuffer


data CompactorReturn = CompactorReturn
  { deltaRetItems :: !Int
  , deltaNominalSize :: !Int
  }

data ReqCompactor (lgWeight :: Nat) = ReqCompactor 
  -- Configuration constants
  { rcRankAccuracy :: !RankAccuracy
  -- State
  , rcState :: !Word64
  , rcSectionSizeFlt :: !Double
  , rcSectionSize :: !Word32
  , rcNumSections :: !Word8
  }


compact :: PrimMonad m => ReqCompactor k -> CompactorReturn -> m DoubleBuffer
compact = undefined

getBuffer :: PrimMonad m => ReqCompactor k -> m DoubleBuffer
getBuffer = undefined

getCoin :: MonadIO m => m Bool
getCoin = uniformR

getLgWeight :: PrimMonad m => m Word8
getLgWeight = undefined

getNominalCapacity :: PrimMonad m => m Int
getNominalCapacity = undefined

getNumSections :: PrimMonad m => m Int
getNumSections = undefined

merge :: PrimMonad m => ReqCompactor lgWeight -> ReqCompactor lgWeight -> m (ReqCompactor lgWeight)
merge = undefined

