{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where
import Data.Char (toLower)
import DataFrame
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
-- Convert both names to lowercase and then compare
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing  -- If the database is empty, return Nothing
findTableByName ((tableName, dataFrame) : rest) name
  | map toLower tableName == map toLower name = Just dataFrame  -- Case-insensitive comparison
  | otherwise = findTableByName rest name  -- Otherwise, search in the rest of the database


-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame _ = error "validateDataFrame ot implemented"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable width (DataFrame cols rows) =
    let colWidth = width `div` fromIntegral (length cols)
    in unwords ["|" ++ cname ++ replicate (fromInteger colWidth - length cname - 2) ' ' | Column cname _ <- cols] ++ "\n" ++
       take (fromInteger width) (cycle ("|" ++ replicate (fromInteger colWidth - 1) '-'))
       ++ "\n" ++
       unlines [unwords ["|" ++ renderValue colWidth v | v <- row] | row <- rows]

renderValue :: Integer -> Value -> String
renderValue width (IntegerValue v) = renderValue' width (show v)
renderValue width (StringValue v) = renderValue' width v
renderValue width (BoolValue v) = renderValue' width (show v)
renderValue width NullValue = renderValue' width "NULL"

renderValue' :: Integer -> String -> String
renderValue' width v
    | length v > fromInteger width = take (fromInteger width - 5) v ++ "..."
    | otherwise = v ++ replicate (fromInteger width - length v - 2) ' '
