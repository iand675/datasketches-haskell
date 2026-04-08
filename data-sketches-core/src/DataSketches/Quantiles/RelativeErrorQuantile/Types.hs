module DataSketches.Quantiles.RelativeErrorQuantile.Types
  ( RankAccuracy(..)
  , DoubleIsNonFiniteException(..)
  ) where

import Control.Exception (Exception)

data RankAccuracy
  = HighRanksAreAccurate
  -- ^ High ranks are prioritized for better accuracy.
  | LowRanksAreAccurate
  -- ^ Low ranks are prioritized for better accuracy
  deriving (Show, Eq)

newtype DoubleIsNonFiniteException = DoubleIsNonFiniteException Double
  deriving (Show, Eq)

instance Exception DoubleIsNonFiniteException
