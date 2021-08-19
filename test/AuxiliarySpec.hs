module AuxiliarySpec where

import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary as Aux
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Test.Hspec
import qualified Data.Vector.Unboxed as U
import qualified Data.List
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer


spec :: Spec
spec = do
  mapM_ checkMergeSortIn [HighRanksAreAccurate, LowRanksAreAccurate]

checkMergeSortIn :: RankAccuracy -> Spec
checkMergeSortIn ra = specify ("mergeSortIn works. hra=" ++ show ra) $ do
  let hraBool = case ra of
        HighRanksAreAccurate -> True
        LowRanksAreAccurate -> False
  let someItems = [1,3..12] -- 6 items

  buf1 <- mkBuffer 25 0 hraBool
  mapM_ (append buf1) someItems
  
  buf2 <- mkBuffer 25 0 hraBool
  mapM_ (append buf2) someItems
  let n = 12

  auxBuilder <- undefined
  Aux.mergeSortIn aux buf1 1 0
  Aux.mergeSortIn aux buf2 2 6
  aux <- mkAuxiliary ra n _ _
  fst (unzip $ raWeightedItems aux) `shouldBe` U.fromList (Data.List.sort (someItems ++ someItems))
