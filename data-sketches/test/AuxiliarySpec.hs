module AuxiliarySpec where

import Data.Primitive.MutVar
import qualified Data.Vector.Unboxed.Mutable as MUVector
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Auxiliary as Aux
import DataSketches.Quantiles.RelativeErrorQuantile.Types
import Test.Hspec
import qualified Data.Vector.Unboxed as U
import qualified Data.List
import qualified Data.Vector as Vector
import DataSketches.Quantiles.RelativeErrorQuantile.Internal.DoubleBuffer
import qualified DataSketches.Quantiles.RelativeErrorQuantile.Internal.Compactor as Compactor
import System.Random.MWC (create)


spec :: Spec
spec = do
  mapM_ checkMergeSortIn [HighRanksAreAccurate, LowRanksAreAccurate]
  describe "issue #2: invariant violated: lastWeight does not equal raSize" $ do
    -- An async exception delivered mid-'insert' (e.g. from a warp request
    -- handler being killed on client disconnect) can leave the sketch's
    -- cached totalN / retainedItems out of sync with the actual compactor
    -- buffer contents. mkAuxiliary must derive both from the compactors
    -- rather than trusting the stale cache, so that quantile() keeps
    -- working instead of throwing.
    specify "mkAuxiliary tolerates stale totalN/retainedItems (derives from compactors)" $ do
      g <- create
      compactor <- Compactor.mkReqCompactor g 0 HighRanksAreAccurate 6
      buff <- Compactor.getBuffer compactor
      mapM_ (append buff) [1 .. 10 :: Double]
      -- Pass deliberately wrong totalN (999) and retainedItems (3); the
      -- old code would either 'error' on the invariant check or
      -- unsafeWrite past the 3-slot vector. The fixed code ignores both
      -- and computes raSize=10, retained=10 from the compactor itself.
      aux <- mkAuxiliary HighRanksAreAccurate 999 3 (Vector.singleton compactor)
      raSize aux `shouldBe` 10
      U.length (raWeightedItems aux) `shouldBe` 10
      Aux.getQuantile aux 0.5 (:<) `shouldSatisfy` (\q -> q >= 1 && q <= 10)
    specify "mkAuxiliary handles empty compactor set" $ do
      aux <- mkAuxiliary HighRanksAreAccurate 0 0 Vector.empty
      raSize aux `shouldBe` 0
      U.length (raWeightedItems aux) `shouldBe` 0

checkMergeSortIn :: RankAccuracy -> Spec
checkMergeSortIn ra = specify ("mergeSortIn works. hra=" ++ show ra) $ do
  let hraBool = case ra of
        HighRanksAreAccurate -> True
        LowRanksAreAccurate -> False
  let oddItems = [1,3..11] -- 6 items
  let evenItems = [2,4..12]

  buf1 <- mkBuffer 25 0 hraBool
  mapM_ (append buf1) oddItems
  
  buf2 <- mkBuffer 25 0 hraBool
  mapM_ (append buf2) evenItems

  let n = 12
  weightedItems <- newMutVar =<< MUVector.new 25  
  let aux = MReqAuxiliary weightedItems ra n
  Aux.mergeSortIn aux buf1 1 0
  Aux.mergeSortIn aux buf2 2 6
  (items, _) <- fmap U.unzip . U.freeze =<< readMutVar (mraWeightedItems aux)
  let itemsToCheck = U.slice 0 (fromIntegral n) items
  itemsToCheck `shouldBe` U.fromList (Data.List.sort (oddItems ++ evenItems))
