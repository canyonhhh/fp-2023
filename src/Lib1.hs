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
import Data.List (transpose)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> TableName -> Maybe DataFrame
findTableByName [] _ = Nothing  -- If the database is empty, return Nothing
findTableByName ((tableName, dataFrame) : rest) name
  | tableName == name = Just dataFrame  -- Case-sensitive comparison
  | otherwise = findTableByName rest name  -- Otherwise, search in the rest of the database


-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
removeSemicolons :: String -> String
removeSemicolons = filter (/= ';')

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement sql = case map toLower (unwords . words $ sql) of
    ('s':'e':'l':'e':'c':'t':' ':'*':' ':'f':'r':'o':'m':' ':rest) -> 
        case words rest of
            (tableName:_) -> Right (removeSemicolons tableName)
            _ -> Left "Error. Missing table name"
    _ -> Left "Invalid SQL statement: Missing 'SELECT * FROM' statement"

-- this task is taken by Marijonas
-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) =
  let
    checkValueType :: Value -> ColumnType -> Bool   -- Helper function to check if a Value matches the specified ColumnType
    checkValueType (IntegerValue _) IntegerType = True
    checkValueType (StringValue _) StringType = True
    checkValueType (BoolValue _) BoolType = True
    checkValueType NullValue _ = True
    checkValueType _ _ = False

    validateColumn :: Column -> [Value] -> Bool   -- Check if all values in a column have the correct type
    validateColumn (Column _ colType) =
      all (`checkValueType` colType)

    validateRowSize :: [Value] -> Bool    -- Check if all rows have the correct number of columns
    validateRowSize values = length values == length columns
   
    columnChecks = zipWith (curry (\(Column name colType, values) -> (name, validateColumn (Column name colType) values))) columns (transpose rows)
   -- Check columns (headers)
    
    rowChecks = map validateRowSize rows    -- Check each row
   
    allChecks = all snd columnChecks && and rowChecks    -- Combine all checks

    generateError
     | and rowChecks = "Error was the column type mismatch"
     | all snd columnChecks    = "Error was the row size mismatch"
     | otherwise               = "Error was the row size mismatch and the column type mismatch"
    
  in
    if allChecks then
      Right ()
    else
      Left generateError


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
        rowsData = if null rows
                   then ""
                   else unlines [unwords ["|" ++ renderValue w v | (v, w) <- zip row colWidths] ++ "|" | row <- rows]
    in header ++ "\n" ++ separator ++ "\n" ++ rowsData

calculateColumnWidths :: Integer -> [Column] -> [Row] -> [Int]
calculateColumnWidths totalWidth cols rows =
    let initialWidths = [max (length cname) (if null rows then 0 else maximum (map (valueLength . flip (!!) idx) rows)) | (Column cname _, idx) <- zip cols [0..]]
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
