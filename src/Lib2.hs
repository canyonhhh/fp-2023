{-# OPTIONS_GHC -Wno-unused-top-binds #-}

-- SHOW TABLES
-- SHOW TABLE name
-- Parse select statemets with:
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

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame)
import InMemoryTables (TableName)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = ParsedStatement

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement _ = Left "Not implemented: parseStatement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
