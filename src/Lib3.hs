{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Lib2
import Data.Time (UTCTime)
import Data.Yaml (decodeEither', encode)
import qualified Data.ByteString as BS

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  | ParseDataFrame FileContent (DataFrame -> next)
  | FilterRows (Maybe Condition) DataFrame (DataFrame -> next)
  | JoinTables [(Aggregate, Maybe TableName, Cname)] (Maybe Condition) DataFrame DataFrame (DataFrame -> next)
  | InsertInto DataFrame Row (DataFrame -> next)
  | DeleteFrom TableName (Maybe Condition) (Either ErrorMessage () -> next)
  | ShowTables (Either ErrorMessage [TableName] -> next)
  | ShowTableStructure TableName (Either ErrorMessage DataFrame -> next)
  | SaveTableData TableName DataFrame (Either ErrorMessage () -> next)
  | AggregateData [(Aggregate, Maybe TableName, Cname)] DataFrame (DataFrame -> next)
  | ReportError ErrorMessage next
  | SelectColumns [(Aggregate, Maybe TableName, Cname)] DataFrame (DataFrame -> next)
  | ParseRow [Value] (Row -> next)
  | UpdateTableDataFrame DataFrame [(Cname, Value)] (Maybe Condition) (DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

parseDataFrame :: FileContent -> Execution DataFrame
parseDataFrame content = liftF $ ParseDataFrame content id

filterRows :: Maybe Condition -> DataFrame -> Execution DataFrame
filterRows condition df = liftF $ FilterRows condition df id

aggregateData :: [(Aggregate, Maybe TableName, Cname)] -> DataFrame -> Execution DataFrame
aggregateData columns df = liftF $ AggregateData columns df id

selectColumns :: [(Aggregate, Maybe TableName, Cname)] -> DataFrame -> Execution DataFrame
selectColumns columns df = liftF $ SelectColumns columns df id

joinTables :: [(Aggregate, Maybe TableName, Cname)] -> Maybe Condition -> DataFrame -> DataFrame -> Execution DataFrame
joinTables columns cond df1 df2 = liftF $ JoinTables columns cond df1 df2 id

insertInto :: DataFrame -> Row -> Execution DataFrame
insertInto df row = liftF $ InsertInto df row id

saveTableData :: TableName -> DataFrame -> Execution (Either ErrorMessage ())
saveTableData tableName df = liftF $ SaveTableData tableName df id

parseRow :: [Value] -> Execution Row
parseRow values = liftF $ ParseRow values id

updateTable :: DataFrame -> [(Cname, Value)] -> Maybe Condition -> Execution DataFrame
updateTable df updates condition = liftF $ UpdateTableDataFrame df updates condition id

-- Function to serialize a DataFrame to YAML
serializeDataFrame :: DataFrame -> BS.ByteString
serializeDataFrame = encode

-- Function to deserialize YAML to a DataFrame
deserializeDataFrame :: BS.ByteString -> Either String DataFrame
deserializeDataFrame bs = case decodeEither' bs of
    Left err -> Left (show err)
    Right df -> Right df

-- Placeholder function to execute SQL commands
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Left errMsg -> return $ Left errMsg
    Right stmt -> case stmt of
        ShowTable tableName -> do
            fileContent <- loadFile tableName
            df <- parseDataFrame fileContent
            return $ Right df
        Lib2.ShowTables -> do
            fileContent <- loadFile "tables.yaml"
            df <- parseDataFrame fileContent
            return $ Right df
        SelectAll tableName -> do
            fileContent <- loadFile tableName
            df <- parseDataFrame fileContent
            return $ Right df
        Select columms tableNames whereClause -> do
            case length tableNames of
                1 -> do
                    fileContent <- loadFile (head tableNames)
                    df <- parseDataFrame fileContent
                    filteredDf <- filterRows whereClause df
                    aggregateDf <- aggregateData (columns stmt) filteredDf
                    selectedDf <- Lib3.selectColumns (columns stmt) aggregateDf
                    return $ Right selectedDf
                2 -> do
                    fileContent1 <- loadFile (head tableNames)
                    fileContent2 <- loadFile (last tableNames)
                    df1 <- parseDataFrame fileContent1
                    df2 <- parseDataFrame fileContent2
                    filteredDf1 <- filterRows whereClause df1
                    filteredDf2 <- filterRows whereClause df2
                    aggregateDf1 <- aggregateData (columns stmt) filteredDf1
                    aggregateDf2 <- aggregateData (columns stmt) filteredDf2
                    selectedDf <- joinTables (columns stmt) whereClause aggregateDf1 aggregateDf2
                    return $ Right selectedDf
                _ -> return $ Left "Joining more than 2 tables is not supported"
        Insert tableName values -> do
            fileContent <- loadFile tableName
            df <- parseDataFrame fileContent
            newRow <- parseRow values
            newDf <- insertInto df newRow
            _ <- saveTableData tableName newDf
            return $ Right newDf
        Update tableName values whereClause -> do
            fileContent <- loadFile tableName
            df <- parseDataFrame fileContent
            updatedDf <- updateTable df values whereClause
            _ <- saveTableData tableName updatedDf
            return $ Right updatedDf
        Delete tableName whereClause -> do
            fileContent <- loadFile tableName
            df <- parseDataFrame fileContent
            updatedDf <- filterRows whereClause df
            _ <- saveTableData tableName updatedDf
            return $ Right $ DataFrame [] []
