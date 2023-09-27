{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame
import InMemoryTables (TableName)
import Data.List (transpose)



type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

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
    validateColumn (Column _ colType) values =
      all (\value -> checkValueType value colType) values

    validateRowSize :: [Value] -> Bool    -- Check if all rows have the correct number of columns
    validateRowSize values = length values == length columns
   
    columnChecks = map (\(Column name colType, values) -> (name, validateColumn (Column name colType) values)) (zip columns (transpose rows))   -- Check columns (headers)
    
    rowChecks = map (\row -> validateRowSize row) rows    -- Check each row
   
    allChecks = all snd columnChecks && all (==True) rowChecks    -- Combine all checks

    generateError = 
      if all (==True) rowChecks then    -- If the generateError function was called it means one of the tests failed, if this is true it means the other test failed
          "Error was the column type missmatch"
      else if all snd columnChecks then   -- Same concept here
          "Error was the row size missmatch"
      else    -- Both failed
          "Error was the row size missmatch and the column type missmatch"
    
  in
    if allChecks then
      Right ()
    else
      Left (generateError)


-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
