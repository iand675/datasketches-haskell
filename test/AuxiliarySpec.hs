module AuxiliarySpec where

import Data.Primitive.MutVar
import qualified Data.Vector.Unboxed.Mutable as MUVector
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
  weightedItems <- newMutVar =<< MUVector.new 25  
  let aux = MReqAuxiliary weightedItems ra n
  Aux.mergeSortIn aux buf1 1 0
  Aux.mergeSortIn aux buf2 2 6
  weightedItems' <- U.freeze =<< readMutVar (mraWeightedItems aux)
  fst (U.unzip $ weightedItems') `shouldBe` U.fromList (Data.List.sort (someItems ++ someItems))
