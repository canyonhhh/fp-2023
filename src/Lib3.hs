{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    insertInto,
    updateTable,
    deleteFrom
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Lib2
import Data.Time (UTCTime)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  | InsertInto TableName Row (Either ErrorMessage () -> next)
  | UpdateTable TableName [(String, Value)] (Maybe Condition) (Either ErrorMessage () -> next)
  | DeleteFrom TableName (Maybe Condition) (Either ErrorMessage () -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

insertInto :: TableName -> [Value] -> Execution (Either ErrorMessage ())
insertInto tableName values = liftF $ InsertInto tableName values id

updateTable :: TableName -> [(String, Value)] -> Maybe Condition -> Execution (Either ErrorMessage ())
updateTable tableName values whereClause = liftF $ UpdateTable tableName values whereClause id

deleteFrom :: TableName -> Maybe Condition -> Execution (Either ErrorMessage ())
deleteFrom tableName whereClause = liftF $ DeleteFrom tableName whereClause id

-- Placeholder function to execute SQL commands
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Left errMsg -> return $ Left errMsg
    Right stmt -> case stmt of
        Insert tableName values -> executeInsert tableName values
        Update tableName values whereClause -> executeUpdate tableName values whereClause
        Delete tableName whereClause -> executeDelete tableName whereClause
        _ -> return $ Left "Unsupported statement"

executeInsert :: TableName -> [Value] -> Execution (Either ErrorMessage DataFrame)
executeInsert _ _ = return $ Left "Not implemented"

executeUpdate :: TableName -> [(String, Value)] -> Maybe Condition -> Execution (Either ErrorMessage DataFrame)
executeUpdate _ _ _ = return $ Left "Not implemented"

executeDelete :: TableName -> Maybe Condition -> Execution (Either ErrorMessage DataFrame)
executeDelete _ _ = return $ Left "Not implemented"
