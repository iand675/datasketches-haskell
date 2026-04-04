import Test.Hspec
import qualified AuxiliarySpec
import qualified CompactorSpec
import qualified DoubleBufferSpec
import qualified ProofCheckSpec
import qualified RelativeErrorQuantileSpec
import qualified KllSpec
import qualified HyperLogLogSpec
import qualified ThetaSpec
import qualified CountMinSpec
import qualified BugFixSpec
import qualified CrossValidationSpec
import System.Environment
import Test.Hspec.JUnit
import Test.Hspec.JUnit.Config
import Test.Hspec.Runner
import qualified Data.Text as T

main :: IO ()
main =
      getArgs
  >>= readConfig config
  >>= withArgs [] . runSpec specs
  >>= evaluateSummary
  where
    config = defaultConfig
      { configFormat = Just $ junitFormat $
          setJUnitConfigOutputFile "test-results.xml" $
            defaultJUnitConfig (T.pack "data-sketches")
      }
    specs = do
      describe "Auxiliary" AuxiliarySpec.spec
      describe "Compactor" CompactorSpec.spec
      describe "DoubleBuffer" DoubleBufferSpec.spec
      describe "ProofCheck" ProofCheckSpec.spec
      describe "RelativeErrorQuantile" RelativeErrorQuantileSpec.spec
      describe "KLL" KllSpec.spec
      describe "HyperLogLog" HyperLogLogSpec.spec
      describe "Theta" ThetaSpec.spec
      describe "CountMin" CountMinSpec.spec
      describe "BugFix" BugFixSpec.spec
      describe "CrossValidation" CrossValidationSpec.spec

