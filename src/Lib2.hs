{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    Aggregate (..),
    Database,
  )
where

import DataFrame
import Lib1
import InMemoryTables (TableName, database)
import Data.Char (toUpper, isLetter)
import Data.List (isPrefixOf, isSuffixOf)


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
             , tableName :: TableName
             , whereClause :: Maybe Condition}
    deriving (Show, Eq)

-- Helpers
split :: (String -> Bool) -> String -> [String]
split f = map (unwords . words) . mergeSubstrings . scanl (\acc x -> if not (f x) then acc++" "++x else "") "" . words

mergeSubstrings :: [String] -> [String]
mergeSubstrings [] = []
mergeSubstrings [a] = [a]
mergeSubstrings (a:b:rest) = if a `isPrefixOf` b then mergeSubstrings (b:rest) else a:mergeSubstrings (b:rest)

-- Case insensitive string comparison
(==*) :: String -> String -> Bool
(==*) a b = map toUpper a == map toUpper b

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement z =
   if last z /= ';' then Left "Invalid statement" else
   let s = init . unwords $ words z
   in if s ==* "SHOW TABLES" then Right ShowTables 
      else if take 10 s ==* "SHOW TABLE" then Right $ ShowTable (drop 11 s)
      else if take 6 s ==* "SELECT" then parseSelect s
      else Left "Invalid statement"

parseSelect :: String -> Either ErrorMessage ParsedStatement
parseSelect s = Right $ 
   Select (parseColumns s)
          (parseFromClause s)
          (parseWhereClause s)

-- Parse from SUM/MAX/None(column) to [(Aggregate, column)]
parseColumns :: String -> [(Aggregate, String)]
parseColumns s = 
   let columnString = takeWhile (\x -> not (x ==* "FROM")) . words $ s
       columns = if length columnString == 1 
                 then error "Invalid statement"
                 else filter (not . null . snd) $ map (parseColumn . filter (/=',')) . tail $ columnString
   in columns

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

parseFromClause :: String -> TableName
parseFromClause s = 
   let f = dropWhile (\x -> not (x ==* "FROM")) . words $ s
   in if length f == 1 then error "Invalid statement"
      else head . tail $ f

parseWhereClause :: String -> Maybe Condition
parseWhereClause s = 
   let w = dropWhile (\x -> not (x ==* "WHERE")) . words $ s
   in if null w then Nothing  -- Parse "cname IS TRUE/FALSE" into list of tuples [(cname, True/False), ...]
      else Just . parseCondition . map ((\ [x, y] -> (x, y)) . words . unwords . split (==*"IS")) . split(==*"AND") . unwords . tail $ w

-- Recursively parse a list of tuples [(cname, True/False), ...] into a Condition
parseCondition :: [(String, String)] -> Condition
parseCondition [] = error "Empty condition"
parseCondition [(cname, bool)] = BoolCondition cname ((head . words $ bool) ==* "TRUE")
parseCondition ((cname, bool):xs) = And (BoolCondition cname ((head . words $ bool) ==* "TRUE")) (parseCondition xs)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- First the where clause is applied to the specified columns
-- Then the aggregate functions are applied to the columns specified in the select statement
-- The columns are then filtered to only include the columns specified in the select statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = showTables
executeStatement (ShowTable name) = showTable name
executeStatement (Select columns tableName whereClause) = 
    filterColumns columns . executeAggregates columns . executeWhereClauses whereClause . findTableByName InMemoryTables.database $ tableName

showTables :: Either ErrorMessage DataFrame
showTables = Right $ DataFrame [Column "Tables_in_database" StringType] [[StringValue a] | (a, _) <- InMemoryTables.database]

showTable :: TableName -> Either ErrorMessage DataFrame
showTable tableName = case findTableByName InMemoryTables.database tableName of
    Just df -> Right df
    Nothing -> Left "Table not found"

executeWhereClauses :: Maybe Condition -> Maybe DataFrame -> Either ErrorMessage DataFrame
executeWhereClauses _ Nothing = Left "Table not found"
executeWhereClauses _ _ = Left "Not implemented"

executeAggregates :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
executeAggregates _ (Left m) = Left m
executeAggregates _ _ = Left "Not implemented"

filterColumns :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
filterColumns _ (Left m) = Left m
filterColumns _ _ = Left "Not implemented"
