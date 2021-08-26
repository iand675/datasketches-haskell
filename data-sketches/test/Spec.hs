import Test.Hspec
import qualified AuxiliarySpec
import qualified CompactorSpec
import qualified DoubleBufferSpec
import qualified ProofCheckSpec
import qualified RelativeErrorQuantileSpec

main :: IO ()
main = hspec $ do
  describe "Auxiliary" AuxiliarySpec.spec
  describe "Compactor" CompactorSpec.spec
  describe "DoubleBuffer" DoubleBufferSpec.spec
  describe "ProofCheck" ProofCheckSpec.spec
  describe "RelativeErrorQuantile" RelativeErrorQuantileSpec.spec

