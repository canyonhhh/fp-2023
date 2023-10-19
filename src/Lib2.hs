{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    Aggregate (..),
    Database,
    showTables,
    showTable,
    applyWhereClauses,
    applyAggregates,
    selectColumns
  )
where

import DataFrame
import Lib1
import InMemoryTables (TableName, database)
import Data.Char (toUpper, isLetter)
import Data.List (isPrefixOf, isSuffixOf, elemIndex)

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
parseSelect s = do
    columns <- parseColumns s
    from <- parseFromClause s
    where_ <- parseWhereClause s
    return $ Select columns from where_

-- Parse from SUM/MAX/None(column) to [(Aggregate, column)]
parseColumns :: String -> Either ErrorMessage [(Aggregate, String)]
parseColumns s = 
   let columnString = takeWhile (\x -> not (x ==* "FROM")) . words $ s
   in if length columnString == 1 
                 then Left "Invalid statement"
                 else Right $ filter (not . null . snd) $ map (parseColumn . filter (/=',')) . tail $ columnString


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

parseFromClause :: String -> Either ErrorMessage TableName
parseFromClause s = 
   let f = dropWhile (\x -> not (x ==* "FROM")) . words $ s
   in if length f == 1 then Left "Invalid statement"
      else Right $ head . tail $ f

parseWhereClause :: String -> Either ErrorMessage (Maybe Condition)
parseWhereClause s = 
   let w = dropWhile (\x -> not (x ==* "WHERE")) . words $ s
   in if null w then Right Nothing  -- Parse "cname IS TRUE/FALSE" into list of tuples [(cname, True/False), ...]
      else case parseCondition . map ((\[x, y] -> (x, y)) . words . unwords . split (==*"IS")) . split(==*"AND") . unwords . tail $ w of
          Left err -> Left err
          Right condition -> Right $ Just condition

-- Recursively parse a list of tuples [(cname, True/False), ...] into a Condition
parseCondition :: [(String, String)] -> Either ErrorMessage Condition
parseCondition [] = Left "Empty condition"
parseCondition [(cname, bool)] = Right $ BoolCondition cname ((head . words $ bool) ==* "TRUE")
parseCondition ((cname, bool):xs) = case parseCondition xs of
    Left err -> Left err
    Right condition -> Right $ And (BoolCondition cname ((head . words $ bool) ==* "TRUE")) condition

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- First the where clause is applied to the specified columns
-- Then the aggregate functions are applied to the columns specified in the select statement
-- The columns are then filtered to only include the columns specified in the select statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = showTables InMemoryTables.database
executeStatement (ShowTable name) = showTable InMemoryTables.database name
executeStatement (Select columns tableName whereClause) = 
    selectColumns columns . applyAggregates columns . applyWhereClauses whereClause . findTableByName InMemoryTables.database $ tableName

showTables :: Database -> Either ErrorMessage DataFrame
showTables db = Right $ DataFrame [Column "Tables_in_database" StringType] [[StringValue a] | (a, _) <- db]

showTable :: Database -> TableName -> Either ErrorMessage DataFrame
showTable db tableName = case findTableByName db tableName of
    Just df -> Right df
    Nothing -> Left "Table not found"

applyWhereClauses :: Maybe Condition -> Maybe DataFrame -> Either ErrorMessage DataFrame
applyWhereClauses _ Nothing = Left "Table not found"
applyWhereClauses _ _ = Left "Not implemented"

applyAggregates :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyAggregates _ (Left m) = Left m
applyAggregates _ _ = Left "Not implemented"


selectColumns :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
selectColumns _ (Left m) = Left m
selectColumns criteria (Right (DataFrame columns rows))
  | null columns && null rows = Left "Empty table"
  | otherwise =
    let validColumns = filter (\(_, colName) -> any (\(Column name _) -> colName == name) columns) criteria
        validColumnIndices = map (\(_, colName) -> getIndex colName columns) validColumns
        missingColumns = filter (\(_, colName) -> colName `notElem` map (\(Column name _) -> name) columns) criteria
        errorMessage = if not (null missingColumns)
                       then "Column " ++ snd (head missingColumns) ++ " not found"
                       else ""
        filteredColumns = [columns !! idx | idx <- validColumnIndices]
        filteredRows = map (\row -> [row !! idx | idx <- validColumnIndices]) rows
    in if null errorMessage
       then Right (DataFrame filteredColumns filteredRows)
       else Left errorMessage

getIndex :: String -> [Column] -> Int
getIndex name columns = case elemIndex (Column name StringType) columns of
  Just idx -> idx
  Nothing  -> error ("Column " ++ name ++ " not found")
