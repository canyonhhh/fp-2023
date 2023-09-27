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
    let adjustedWidth = width - fromIntegral (length cols + 2)
        colWidths = calculateColumnWidths adjustedWidth cols rows
        header = unwords ["|" ++ cname ++ replicate (w - length cname) ' ' | (Column cname _, w) <- zip cols colWidths] ++ "|"
        separator = unwords ["|" ++ replicate w '-' | w <- colWidths] ++ "|"
    in header ++ "\n" ++ separator ++ "\n" ++
       unlines [unwords ["|" ++ renderValue w v | (v, w) <- zip row colWidths] ++ "|" | row <- rows]

calculateColumnWidths :: Integer -> [Column] -> [Row] -> [Int]
calculateColumnWidths totalWidth cols rows =
    let initialWidths = [max (length cname) (maximum (map (valueLength . flip (!!) idx) rows)) | (Column cname _, idx) <- zip cols [0..]]
        adjustWidths ws
            | sum ws <= fromInteger totalWidth = ws
            | otherwise = adjustWidths (map (\w -> if w > 2 then w - 1 else w) ws)
    in adjustWidths initialWidths

valueLength :: Value -> Int
valueLength (IntegerValue v) = length (show v)
valueLength (StringValue v) = length v
valueLength (BoolValue v) = length (show v)
valueLength NullValue = 4 -- "NULL"

renderValue :: Int -> Value -> String
renderValue width (IntegerValue v) = renderValue' width (show v)
renderValue width (StringValue v) = renderValue' width v
renderValue width (BoolValue v) = renderValue' width (show v)
renderValue width NullValue = renderValue' width "NULL"

renderValue' :: Int -> String -> String
renderValue' width v
    | length v > width = take (width - 3) v ++ "..."
    | otherwise = v ++ replicate (width - length v) ' '
