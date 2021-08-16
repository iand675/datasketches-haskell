module DataSketches.Quantiles.RelativeErrorQuantile.Types where

data Criterion = (:<) | (:<=)

data RankAccuracy = HighRanksAreAccurate | LowRanksAreAccurate
  deriving (Eq)