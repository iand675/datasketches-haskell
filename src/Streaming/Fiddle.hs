{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Streaming.Fiddle 
  ( BiasedQuantileTree
  , bqt
  , insert
  , compress
  , query
  -- * Internals
  , leftCount
  , ancestorCount
  , approximateRank
  ) where

import qualified Data.FingerTree as F
import Data.FingerTree (Measured, (<|), (|>), (><), ViewL(..), ViewR(..))
import Data.Monoid (Sum (..))
import Prelude hiding (sum)
import Data.Semigroup (Semigroup, Max(..), Min(..))
import Prelude (Foldable)

data Node a = Node 
  { nodeValue :: a
  , nodeCount :: Double
  }
  deriving (Show, Eq)

node :: a -> Node a
node x = Node x 1

incrementNodeCount :: Node a -> Node a
incrementNodeCount n = n { nodeCount = nodeCount n + 1 }

data Summary a = Summary
  { sCount :: Sum Double
  -- , rank ?
  , sMax :: Max a
  , sMin :: Min a
  }

instance (Ord a, Num a) => Semigroup (Summary a) where
  (Summary c1 max1 min1) <> (Summary c2 max2 min2) = Summary (c1 <> c2) (max1 <> max2) (min1 <> min2)

instance (Ord a, Num a) => Monoid (Summary a) where
  mempty = Summary mempty (Max 0) (Min 0)

instance (Ord a, Num a) => Measured (Summary a) (Node a) where
  measure (Node x c) = Summary (Sum c) (Max x) (Min 0)


leftCount :: (Ord a, Num a) => a -> BiasedQuantileTree a -> Double
leftCount x BiasedQuantileTree{..} = case F.split (\s -> getMax (sMax s) > x) (biasedQuantileLeaves >< biasedQuantileTree) of
  (ls, _) -> getSum $ sCount $ F.measure ls

ancestorCount :: Num a => a -> BiasedQuantileTree a -> Double
ancestorCount = undefined

-- | Referred to as r̂ in the paper, this is an approximation of rank(x)
-- such that (for an accuracy parameter ε, supplied in advance):
-- rank(x) − εN ≤ r̂(x) ≤ rank(x) + εN
--
-- Where N is the size of the data stream being inserted into the BiasedQuantileTree
approximateRank :: (Ord a, Num a) => a -> BiasedQuantileTree a -> Double
approximateRank x bt = leftCount x bt - (ancestorCount x bt / 2)

-- | Many queries over large data sets require non-uniform responses. 
-- Consider published lists of wealth distributions: one typically sees details of the median income, 
-- the 75th percentile, 90th, 95th and 99th percentiles, and a list of the 500 top earners. 
-- While the detail around the center of the distribution is quite sparse, at one end of the 
-- distribution we see increasingly fine gradations in the accuracy of the response, ultimately down 
-- to the level of individuals. Similar variations in the level of accuracy required are seen in 
-- analyzing massive data streams: for example, in monitoring performance in packet networks, 
-- the distribution of round trip times is used to analyze the quality of service. 
-- Again, it is important to know broad information about the center of the distribution (median, quartiles),
-- and increasingly precise information about the tail of the distribution, since this is most 
-- indicative of unusual or anomalous behavior in the network. A particular challenge is how to maintain 
-- such distributional information in a data streaming scenario, where massive amounts of data are seen 
-- at very high speeds: the requirement is to summarize this huge volume of data into small space to be 
-- able to accurately capture the key features of the distribution, within allowable approximation bounds.
-- 
-- Under the hood, this implementation is a partitioned finger-tree that maintains the following 
-- invariants:
--
-- BiasedQuantileTree = biasedQuantileTree ∪ biasedQuantileLeaves 
-- biasedQuantileTree ∩ biasedQuantileLeaves = ∅.
--
-- We will maintain the additional following properties:
--
-- 1. ∀x∈[a]: leftCount(x) − ancestorCount(x) ≤ rank(x) ≤ leftCount(x)
-- 2. ∀v∈bq: v̸=lf(v)⇒cv ≤ α * L(v)
-- 3.
-- 4. maximum biasedQuantileLeaves < minimum biasedQuantileTree
-- 5. all the leaves stored in biasedQuantileLeaves occur to the left of all the tree nodes stored in biasedQuantileTree. 
-- 6.

data BiasedQuantileTree a = BiasedQuantileTree 
  { biasedQuantileErrorTerm :: {-# UNPACK #-} !Double
    -- ^ Denoted as ε in the paper, this represents the acceptable percentage of error
  , biasedQuantileLeaves :: F.FingerTree (Summary a) (Node a) 
  , biasedQuantileTree :: F.FingerTree (Summary a) (Node a)
  }
  deriving (Show, Eq)

-- count :: Store a -> Double
-- count = getSum . sCount . F.measure . tree

summaryCount :: (Num a, Ord a) => F.FingerTree (Summary a) (Node a) -> Double
summaryCount = getSum . sCount . F.measure 

bqt :: (Num a, Ord a) => Double -> BiasedQuantileTree a
bqt errorTerm = BiasedQuantileTree errorTerm mempty mempty

maxL :: (Ord a, Num a) => BiasedQuantileTree a -> a
maxL b = case F.viewr $ biasedQuantileLeaves b of
  EmptyR -> 0
  _ :> n -> nodeValue n

-- | We next describe how to maintain the data structure in the presence of arrivals of updates. 
-- The INSERT (x) procedure takes a new item x and includes it in bq. We first determine whether 
-- to include x in bql or bqt: if x ≤ maxu∈bql or |bqt| = 0, then we insert x into bql: 
-- if x is already present in bql then we increment cx; else we insert x into bql and set cx =1. 
-- If we don’t put x in bql, we will insert x into bqt: we will find the closest ancestor of x in bqt,
-- v, and update the count of v, if this does not violate condition (2) — if it would, 
-- then we insert the descendant of v that is an ancestor of x into bqt and set its count to 1. 
-- This routine is illustrated in pseudo-code in Figure 2.
insert :: forall a. (Show a, Ord a, Num a) => a -> BiasedQuantileTree a -> BiasedQuantileTree a
insert x b@BiasedQuantileTree{..} = case F.viewr biasedQuantileLeaves of
  -- biasedQuantileLeaves is empty, just create a single item within it.
  EmptyR -> b { biasedQuantileLeaves = F.singleton $ node x }
  bql :> n -> case compare x (nodeValue n) of
    -- In this case, x should go into biasedQuantileLeaves, but we need to figure out where. 
    LT -> case F.search bqlPredicate biasedQuantileLeaves of 
      F.Position l n r -> case compare x (nodeValue n) of
        LT -> b { biasedQuantileLeaves = l >< (node x <| n <| r)}
        EQ -> b { biasedQuantileLeaves = l >< (incrementNodeCount n <| r) }
        GT -> b { biasedQuantileLeaves = l >< (n <| node x <| r)}
      F.OnLeft -> b { biasedQuantileLeaves = node x <| biasedQuantileLeaves }
      F.OnRight -> b { biasedQuantileLeaves = biasedQuantileLeaves |> node x }
      F.Nowhere -> error "Sad"
    -- In this case, x is equal to the max value of biasedQuantileLeaves, so we can just increment the count
    EQ -> b { biasedQuantileLeaves = bql |> incrementNodeCount n }
    -- In this case, x belongs in biasedQuantileTree instead.
    --
    -- We divide the nodes v ∈ V into a sequence of equivalence classes based on the leftCount value 
    -- computed for v.  That is, group together all nodes u, v ∈ V for which L(u) = L(v). 
    GT -> error "not implemented"
  where
    -- attempting to find a point where a predicate on splits of the sequence changes from False to True.
    -- This is a bit counterintuitive, but it helps us find the correct insertion point for the biasedQuantileLeaves
    -- tree.
    bqlPredicate :: Summary a -> Summary a -> Bool
    bqlPredicate l r = 
      x < getMax (sMax l) &&
      x >= getMin (sMin r)

 
      
  
-- | The compress procedure takes the data structure, and ensures that conditions (1)–(6) hold,
-- ensuring that the data structure remains accurate for answering queries, but additionally 
-- the space used is tightly bounded. The procedure has several steps:
--
-- First, we reduce the size of bql to its smallest permitted size, 
-- and insert the leaves that are removed from bql into bqt. 
--
-- We then recompute L values for all nodes in bqt to reflect the insertions that have happened, 
-- and then we compress each subtree within bqt by reallocating the weights.
compress :: BiasedQuantileTree a -> BiasedQuantileTree a
compress = undefined

query :: BiasedQuantileTree a -> a
query = undefined