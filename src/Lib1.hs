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
    -- Helper function to check if a Value matches the specified ColumnType
    checkValueType :: Value -> ColumnType -> Bool
    checkValueType (IntegerValue _) IntegerType = True
    checkValueType (StringValue _) StringType = True
    checkValueType (BoolValue _) BoolType = True
    checkValueType NullValue _ = True
    checkValueType _ _ = False

    -- Check if all values in a column have the correct type
    validateColumn :: Column -> [Value] -> Bool
    validateColumn (Column _ colType) values =
      all (\value -> checkValueType value colType) values

    -- Check if all rows have the correct number of columns
    validateRowSize :: [Value] -> Bool
    validateRowSize values = length values == length columns

    -- Check each column
    columnChecks = map (\(Column name colType, values) -> (name, validateColumn (Column name colType) values)) (zip columns (transpose rows)) -- This line: map takes a function and a list as the arguments
    -- (zip columns (transpose rows)) is the list, zip takes two lists and zips them one with other into tuples, and transpose rows basically takes every rows equiv element essentially just switching them to columns without the data type and name block
    -- so then zip just combines the column name and type
    -- Check each row
    rowChecks = map (\row -> validateRowSize row) rows

    -- Combine all checks
    allChecks = all snd columnChecks && all (==True) rowChecks

    generateError = 
      if all (==True) rowChecks then  -- if the generateError function was called it means one of the tests failed, if this is true it means the other test failed
          "Error was the column type missmatch"
      else if all snd columnChecks then -- same concept here
          "Error was the row size missmatch"
      else  -- Both failed
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
