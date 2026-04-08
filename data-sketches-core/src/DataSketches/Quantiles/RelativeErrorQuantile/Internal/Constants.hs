module DataSketches.Quantiles.RelativeErrorQuantile.Internal.Constants where

initNumberOfSections :: Num a => a
initNumberOfSections = 3

relRseFactor :: Double
relRseFactor = sqrt (0.0512 / fromIntegral initNumberOfSections)

fixRseFactor :: Double
fixRseFactor = 0.084
