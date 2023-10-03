{-# OPTIONS_GHC -Wno-unused-top-binds #-}

-- SHOW TABLES
-- SHOW TABLE name
-- Parse select statemets with:
-- column list:
   -- Parse and recognize column names in a given query.
   -- Return the specified columns from the table in the result.
   -- Ensure provided column names exist in the table.
-- max
   -- Parse the MAX aggregate function.
   -- Return the largest value in the specified column.
   -- Ensure that only integers, bools, and strings are processed.
-- sum
   -- Parse the SUM aggregate function.
   -- Return the sum of all values in the specified column.
   -- Ensure that integers are processed.
-- where AND
   -- Parse multiple conditions combined using AND.
   -- Ensure all conditions combined with AND are met for a row are included in the result.
   -- Aggregate functions can be applied to the results ( MIN, MAX, etc.)
-- where bool is true/false
   -- Filter rows based on whether the specified column's value is TRUE or FALSE.

--module Lib2
  --( parseStatement,
    --executeStatement,
    --ParsedStatement
  --)
--where

import DataFrame
import InMemoryTables (TableName)
import Data.Char (toUpper, isLetter)
import Data.List (isPrefixOf, isSuffixOf)
import Data.List.Split (splitOn)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Condition
    = And Condition Condition
    | BoolCondition String Bool
    deriving (Show, Eq)

data Aggregate
    = Max 
    | Sum 
    | None
    deriving (Show, Eq)

data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | Select { columns :: [(Aggregate, String)]
             , tableName :: Maybe TableName
             , whereClause :: Maybe Condition}
    deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement z =
   let s = unwords $ words z
   in if map toUpper s == "SHOW TABLES" then Right ShowTables 
      else if map toUpper (take 10 s) == "SHOW TABLE" then Right $ ShowTable (drop 11 s)
      else if map toUpper (take 6 s) == "SELECT" then parseSelect s
      else Left "Invalid statement"

parseSelect :: String -> Either ErrorMessage ParsedStatement
parseSelect s = Right $ 
   Select (parseColumns s)
          (parseFromClause s)
          (parseWhereClause s)

-- Parse from SUM/MAX/None(column) to [(Aggregate, column)]
parseColumns :: String -> [(Aggregate, String)]
parseColumns s = 
   let c = takeWhile (\x -> map toUpper x /= "FROM") . words $ s
   in if length c == 1 then error "Invalid statement"
      else map parseColumn . splitOn "," . unwords . tail $ c

-- Parse SUM/MAX/None(column) to (Aggregate, column)
parseColumn :: String -> (Aggregate, String)
parseColumn s = 
    case () of
        _ | "SUM(" `isPrefixOf` s && ")" `isSuffixOf` s -> (Sum, extractColumnName s)
          | "MAX(" `isPrefixOf` s && ")" `isSuffixOf` s -> (Max, extractColumnName s)
          | otherwise -> (None, filter isLetter s)
    where
        extractColumnName :: String -> String
        extractColumnName = filter isLetter . init . drop 4


parseFromClause :: String -> Maybe TableName
parseFromClause s = 
   let f = dropWhile (\x -> map toUpper x /= "FROM") . words $ s
   in if length f == 1 then Nothing
      else Just . head . tail $ f

parseWhereClause :: String -> Maybe Condition
parseWhereClause s = 
   let w = dropWhile (\x -> map toUpper x /= "WHERE") . words $ s
   in if null w then Nothing  -- Parse "cname IS TRUE/FALSE" into list of tuples [(cname, True/False), ...]
      else Just . parseCondition . map ((\ [x, y] -> (x, y)) . words . unwords . splitOn "IS") . splitOn "AND" . unwords . tail $ w

-- Recursively parse a list of tuples [(cname, True/False), ...] into a Condition
parseCondition :: [(String, String)] -> Condition
parseCondition [] = error "Empty condition"
parseCondition [(cname, bool)] = BoolCondition cname (map toUpper (head . words $ bool) == "TRUE")
parseCondition ((cname, bool):xs) = And (BoolCondition cname (map toUpper (head . words $ bool) == "TRUE")) (parseCondition xs)


-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
