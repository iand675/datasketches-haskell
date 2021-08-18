module DataSketches.Quantiles.RelativeErrorQuantile.Types where
import Control.Monad.Primitive

data Criterion = (:<) | (:<=)
  deriving (Show, Eq)

data RankAccuracy 
  = HighRanksAreAccurate 
  -- ^ High ranks are prioritized for better accuracy.
  | LowRanksAreAccurate
  -- ^ Low ranks are prioritized for better accuracy
  deriving (Show, Eq)

class TakeSnapshot a where
  data Snapshot a
  takeSnapshot :: PrimMonad m => a (PrimState m) -> m (Snapshot a)
