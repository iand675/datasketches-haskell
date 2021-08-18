{-# OPTIONS_GHC -fno-full-laziness #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric #-}

import Control.Monad (forM_)
import Criterion.Main
import Criterion.Types
import Data.List (foldl')
import Control.DeepSeq


main :: IO ()
main = do
  defaultMain
    [ bgroup "ReqSketch"
      [
      ]
    ]