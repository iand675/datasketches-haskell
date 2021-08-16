module DataSketches.Quantiles.RelativeErrorQuantile.Constants where
-- Constants
import Data.Word

sqrt2 :: Double 
sqrt2 = sqrt 2

initNumberOfSections :: Word8
initNumberOfSections = 3

minK :: Word8
minK = 4

nomCapMulti :: Word8
nomCapMulti = 2

relRseFactor :: Double
relRseFactor = sqrt (0.0512 / fromIntegral initNumberOfSections)

fixRseFactor :: Double
fixRseFactor = 0.084
