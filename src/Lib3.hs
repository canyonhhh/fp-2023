{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    TableName,
    FileContent,
    ErrorMessage,
    serializeDataFrame,
    deserializeDataFrame
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Lib2
import Data.Time (UTCTime)
import Data.Yaml (decodeEither')
import qualified Data.ByteString.Char8 as BS
import Data.Char (toLower)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (Either ErrorMessage FileContent -> next)
  | SaveTableData TableName (Either ErrorMessage DataFrame) (Either ErrorMessage () -> next)
  | ParseDataFrame (Either ErrorMessage FileContent) (Either ErrorMessage DataFrame -> next)
  | GetTableNames ([TableName] -> next)
  | GetTime (UTCTime -> next)
  | FilterRows (Maybe Condition) (Either ErrorMessage DataFrame) (Either ErrorMessage DataFrame -> next)
  | JoinTables (Maybe Condition) (Either ErrorMessage DataFrame) (Either ErrorMessage DataFrame)(Either ErrorMessage DataFrame -> next)
  | InsertInto [(Cname, Value)] (Either ErrorMessage DataFrame) (Either ErrorMessage DataFrame -> next)
  | AggregateData [(Aggregate, Cname)] (Either ErrorMessage DataFrame) (Either ErrorMessage DataFrame -> next)
  | SelectColumns [(Aggregate, Cname)] (Either ErrorMessage DataFrame) (Either ErrorMessage DataFrame -> next)
  | UpdateTableDataFrame [(Cname, Value)] (Maybe Condition) (Either ErrorMessage DataFrame)(Either ErrorMessage DataFrame -> next)
  | ReportError ErrorMessage next
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution (Either ErrorMessage FileContent)
loadFile name = liftF $ LoadFile name id

saveTableData :: TableName -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage ())
saveTableData name df = liftF $ SaveTableData name df id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getTableNames :: Execution [TableName]
getTableNames = liftF $ GetTableNames id

parseDataFrame :: Either ErrorMessage FileContent -> Execution (Either ErrorMessage DataFrame)
parseDataFrame fileContent = liftF $ ParseDataFrame fileContent id

filterRows :: Maybe Condition -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
filterRows cond df = liftF $ FilterRows cond df id

aggregateData :: [(Aggregate, Cname)] -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
aggregateData columns df = liftF $ AggregateData columns df id

selectColumns :: [(Aggregate, Cname)] -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
selectColumns columns df = liftF $ SelectColumns columns df id

joinTables :: Maybe Condition -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
joinTables cond df1 df2 = liftF $ JoinTables cond df1 df2 id

insertInto :: [(Cname, Value)] -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
insertInto values df = liftF $ InsertInto values df id

updateTable :: [(Cname, Value)] -> Maybe Condition -> Either ErrorMessage DataFrame -> Execution (Either ErrorMessage DataFrame)
updateTable values cond df = liftF $ UpdateTableDataFrame values cond df id

reportError :: ErrorMessage -> Execution ()
reportError msg = liftF $ ReportError msg ()

loadAndParse :: TableName -> Execution (Either ErrorMessage DataFrame)
loadAndParse tableName = do
    fileContent <- loadFile tableName
    parseDataFrame fileContent

-- Function to serialize a DataFrame to YAML
serializeDataFrame :: Either ErrorMessage DataFrame -> Either ErrorMessage String
serializeDataFrame (Left errMsg) = Left errMsg
serializeDataFrame (Right (DataFrame columns rows)) = Right $
    "columns:\n" ++ serializeColumns columns ++ "\nrows:\n" ++ serializeRows columns rows
    where
        serializeColumns :: [Column] -> String
        serializeColumns = concatMap serializeColumn

        serializeColumn :: Column -> String
        serializeColumn (Column name ctype) = 
            "  - name: " ++ name ++ "\n    type: " ++ show ctype ++ "\n"

        serializeRows :: [Column] -> [[Value]] -> String
        serializeRows cols = concatMap (serializeRow cols)

        serializeRow :: [Column] -> [Value] -> String
        serializeRow cols row = 
            "  - " ++ intercalate "\n    " (zipWith serializeValue cols row) ++ "\n"

        serializeValue :: Column -> Value -> String
        serializeValue (Column name _) val = name ++ ": " ++ serializeDataFrameValue val

        serializeDataFrameValue :: Value -> String
        serializeDataFrameValue (IntegerValue i) = show i
        serializeDataFrameValue (StringValue s) = s
        serializeDataFrameValue (BoolValue b) = map toLower $ show b  -- Convert to lowercase for boolean
        serializeDataFrameValue NullValue = "null"

        intercalate :: String -> [String] -> String
        intercalate _ [] = ""
        intercalate sep (x:xs) = x ++ concatMap (sep ++) xs

deserializeDataFrame :: Either ErrorMessage FileContent -> Either String DataFrame
deserializeDataFrame (Left errMsg) = Left errMsg
deserializeDataFrame (Right bs) = case decodeEither' . BS.pack $ bs of
    Left err -> Left (show err)
    Right df -> Right df

-- Placeholder function to execute SQL commands
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Left errMsg -> return $ Left errMsg
    Right stmt -> case stmt of
        -- returns the columns and data types of the table (without the rows)
        Lib2.ShowTable tableName -> do
            df <- loadAndParse tableName
            case df of
                Left errMsg -> return $ Left errMsg
                Right (DataFrame columns _) -> return $ Right $ DataFrame [Column "Field" StringType, Column "Type" StringType] [[StringValue cname, StringValue (show ctype)] | Column cname ctype <- columns]
        Lib2.ShowTables -> Right . DataFrame [Column "table_name" StringType] . map (\ name -> [StringValue name]) <$> getTableNames
        SelectAll tableName -> loadAndParse tableName
        Select columms tableNames whereClause -> do
            case length tableNames of
                1 -> aggregateData (columns stmt) =<< Lib3.selectColumns (columns stmt) =<< filterRows whereClause =<< loadAndParse (head tableNames)
                2 -> do
                    df1 <- aggregateData (columns stmt) =<< filterRows whereClause =<< loadAndParse (head tableNames)
                    df2 <- aggregateData (columns stmt) =<< filterRows whereClause =<< loadAndParse (last tableNames)
                    joinedDf <- joinTables whereClause df1 df2
                    Lib3.selectColumns (columns stmt) joinedDf
                _ -> return $ Left "Joining more than 2 tables is not supported"
        Insert tableName values -> do
            df <- insertInto values =<< loadAndParse tableName
            _ <- saveTableData tableName df
            return df
        Update tableName values whereClause -> do
            df <-  updateTable values whereClause =<< loadAndParse tableName
            _ <- saveTableData tableName df
            return df
        Delete tableName whereClause -> do
            df <- loadAndParse tableName
            filteredDf <- filterRows whereClause df
            let complementedDf = dataFrameComplement df filteredDf
            _ <- saveTableData tableName complementedDf
            return complementedDf

-- function to get the rows from df1 which don't exist in df2
dataFrameComplement :: Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
dataFrameComplement (Right (DataFrame columns1 rows1)) (Right (DataFrame _ rows2)) =
    Right $ DataFrame columns1 (filter (`notElem` rows2) rows1)
dataFrameComplement (Left errMsg) _ = Left errMsg
dataFrameComplement _ (Left errMsg) = Left errMsg
