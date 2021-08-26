import Test.Hspec
import qualified AuxiliarySpec
import qualified CompactorSpec
import qualified DoubleBufferSpec
import qualified ProofCheckSpec
import qualified RelativeErrorQuantileSpec
import System.Environment
import Test.HSpec.JUnit
import Test.Hspec.Runner

main :: IO ()
main = do
  isCI <- lookupEnv "CI"
  args <- getArgs
  >>= readConfig config
  >>= withArgs [] . runSpec specs
  >>= evaluateSummary
  where
    config = defaultConfig 
      { configFormat = junitFormat "test-results.xml" "data-sketches" 
      }
    specs = do
      describe "Auxiliary" AuxiliarySpec.spec
      describe "Compactor" CompactorSpec.spec
      describe "DoubleBuffer" DoubleBufferSpec.spec
      describe "ProofCheck" ProofCheckSpec.spec
      describe "RelativeErrorQuantile" RelativeErrorQuantileSpec.spec

