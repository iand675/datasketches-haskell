import Test.Hspec
import qualified ProofCheckSpec
import qualified RelativeErrorQuantileSpec
import qualified KllSpec
import qualified HyperLogLogSpec
import qualified ThetaSpec
import qualified CountMinSpec
import qualified BugFixSpec
import qualified CrossValidationSpec
import System.Environment
import Test.Hspec.Runner

main :: IO ()
main =
      getArgs
  >>= readConfig defaultConfig
  >>= withArgs [] . runSpec specs
  >>= evaluateSummary
  where
    specs = do
      describe "ProofCheck" ProofCheckSpec.spec
      describe "RelativeErrorQuantile" RelativeErrorQuantileSpec.spec
      describe "KLL" KllSpec.spec
      describe "HyperLogLog" HyperLogLogSpec.spec
      describe "Theta" ThetaSpec.spec
      describe "CountMin" CountMinSpec.spec
      describe "BugFix" BugFixSpec.spec
      describe "CrossValidation" CrossValidationSpec.spec
