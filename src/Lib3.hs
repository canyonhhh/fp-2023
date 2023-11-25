{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE DeriveFunctor #-} {-# OPTIONS_GHC -Wno-name-shadowing #-}
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
    deserializeDataFrame,
    dataFrameComplement,
    insertInto,
    updateTableDataFrame,
    joinTables,
    matchCondition
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame
import Lib2
import Data.Time (UTCTime)
import Data.Yaml (decodeEither')
import qualified Data.ByteString.Char8 as BS
import Data.Char (toLower)

import qualified Data.Map as M
import Data.Maybe (fromMaybe)
import Data.List qualified as L

type FileContent = String
type ErrorMessage = String
type TableName = String

data ExecutionAlgebra next
    = LoadFile TableName (Either ErrorMessage FileContent -> next)
    | SaveTableData TableName (Either ErrorMessage DataFrame) (Either ErrorMessage () -> next)
    | GetTableNames ([TableName] -> next)
    | GetTime (UTCTime -> next)
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
        serializeDataFrameValue (BoolValue b) = map toLower $ show b
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
        Lib2.ShowTable tableName -> do
            df <- loadAndParse tableName
            return $ showTable df
        Lib2.ShowTables -> do
            Right . DataFrame [Column "table_name" StringType] . map (\ name -> [StringValue name]) <$> getTableNames
        Lib2.SelectAll tableName whereClause -> do
            df <- loadAndParse tableName
            return $ applyWhereClauses whereClause df
        Lib2.Select columns tableNames whereClause -> do
            time <- getTime
            case tableNames of
                [tableName] -> do
                    df <- loadAndParse tableName
                    return $ processSelect columns whereClause time df
                [table1, table2] -> do
                    df1 <- loadAndParse table1
                    df2 <- loadAndParse table2
                    return $ processJoinSelect columns whereClause time df1 df2
                _ -> return $ Left "Joining more than 2 tables is not supported"
        Lib2.Insert tableName values -> do
            df <- loadAndParse tableName
            let updatedDf = insertInto values df
            _ <- saveTableData tableName updatedDf
            return updatedDf
        Lib2.Update tableName values whereClause -> do
            df <- loadAndParse tableName
            let updatedDf = updateTableDataFrame values whereClause df
            _ <- saveTableData tableName updatedDf
            return updatedDf
        Lib2.Delete tableName whereClause -> do
            df <- loadAndParse tableName
            let filteredDf = applyWhereClauses whereClause df
                complementedDf = dataFrameComplement df filteredDf
            _ <- saveTableData tableName complementedDf
            return complementedDf
        Lib2.ShowTime -> do
            time <- getTime
            return $ Right $ DataFrame [Column "NOW()" StringType] [[StringValue $ show time]]

loadAndParse :: TableName -> Execution (Either ErrorMessage DataFrame)
loadAndParse tableName = do
    fileContent <- loadFile tableName
    return $ case deserializeDataFrame fileContent of
        Left err -> Left err
        Right df -> Right df

processSelect :: [ColumnExpression] -> Maybe Condition -> UTCTime -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
processSelect columns whereClause time df = 
    applyTimeFunction columns time $ applyAggregates columns $ selectColumns columns $ applyWhereClauses whereClause df

processJoinSelect :: [ColumnExpression] -> Maybe Condition -> UTCTime -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
processJoinSelect columns whereClause time df1 df2 =
    applyTimeFunction columns time $ applyAggregates columns $ selectColumns columns $ joinTables whereClause df1 df2

-- function to get the rows from df1 which don't exist in df2
dataFrameComplement :: Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
dataFrameComplement (Right (DataFrame columns1 rows1)) (Right (DataFrame _ rows2)) =
    Right $ DataFrame columns1 (filter (`notElem` rows2) rows1)
dataFrameComplement (Left errMsg) _ = Left errMsg
dataFrameComplement _ (Left errMsg) = Left errMsg


matchCondition :: Lib2.Condition -> [Column] -> [Value] -> Bool
matchCondition condition columns row = 
    case condition of
        Lib2.Equal columnName valueToMatch -> 
            let columnIndex = L.findIndex (\(Column name _) -> name == columnName) columns
            in case columnIndex of
                Just idx -> row !! idx == valueToMatch
                Nothing -> False
        Lib2.And cond1 cond2 -> 
            let cond1Result = matchCondition cond1 columns row
                cond2Result = matchCondition cond2 columns row
            in
                cond1Result && cond2Result
        Lib2.BoolCondition cname bool ->
            let columnIndex = L.findIndex (\(Column name _) -> name == cname) columns
                values = map (\(BoolValue v) -> v) row
            in case columnIndex of
                Just idx -> values !! idx == bool
                Nothing -> False
        Lib2.JoinCondition _ _ -> False

joinTables :: Maybe Lib2.Condition -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
joinTables _ (Left errMsg) _ = Left errMsg
joinTables _ _ (Left errMsg) = Left errMsg
joinTables Nothing (Right (DataFrame columns1 rows1)) (Right (DataFrame columns2 rows2)) = Right $ DataFrame (columns1 ++ columns2) [row1 ++ row2 | row1 <- rows1, row2 <- rows2]
joinTables (Just (Lib2.JoinCondition c1 c2)) (Right (DataFrame columns1 rows1)) (Right (DataFrame columns2 rows2)) =
    let
        idx1 = L.findIndex (\(Column name _) -> name == c1) columns
        idx2 = L.findIndex (\(Column name _) -> name == c2) columns
    in case (idx1, idx2) of
        (Just i1, Just i2) -> Right $ DataFrame columns (filter (joinFilter i1 i2) cartesianProduct)
        _ -> Left "One or more columns not found in DataFrame."
    where
        cartesianProduct = [row1 ++ row2 | row1 <- rows1, row2 <- rows2]
        columns = columns1 ++ columns2
joinTables _ _ _ = Left "Invalid join condition."

joinFilter :: Int -> Int -> [Value] -> Bool
joinFilter idx1 idx2 row =
    let value1 = row !! idx1
        value2 = row !! idx2
    in value1 == value2

-- Sort [Value] by [Cname] in the order of [Cname] in [Column]
insertInto :: [(Lib2.Cname, Value)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
insertInto _ (Left err) = Left err
insertInto row (Right (DataFrame columns rows)) = 
    case mapM (getColumnIndex columns) row of
        Just sortedRowIndices -> 
            if all (uncurry matchColumnType) (zip sortedRowIndices row)
            then let sortedRow = map (snd . (row !!)) sortedRowIndices
                 in Right $ DataFrame columns (rows ++ [sortedRow])
            else Left "Type mismatch between row values and DataFrame columns."
        Nothing -> Left "One or more columns not found in DataFrame."
  where
    getColumnIndex :: [Column] -> (Lib2.Cname, Value) -> Maybe Int
    getColumnIndex cols (cname, _) =
        L.findIndex (\(Column name _) -> name == cname) cols

    matchColumnType :: Int -> (Lib2.Cname, Value) -> Bool
    matchColumnType idx (_, value) =
        case (columns !! idx, value) of
            (Column _ IntegerType, IntegerValue _) -> True
            (Column _ StringType, StringValue _) -> True
            (Column _ BoolType, BoolValue _) -> True
            _ -> False


-- Find the row in the DataFrame that matches the condition and update it
updateTableDataFrame :: [(Lib2.Cname, Value)] -> Maybe Lib2.Condition -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
updateTableDataFrame _ _ (Left errMsg) = Left errMsg
updateTableDataFrame updates maybeCondition (Right (DataFrame columns rows)) =
    if allColumnsExist updates columns && allTypesMatch updates columns
    then Right $ DataFrame columns (map (updateRowIfNeeded updates maybeCondition columns) rows)
    else Left "One or more columns not found or type mismatch."

updateRowIfNeeded :: [(Lib2.Cname, Value)] -> Maybe Lib2.Condition -> [Column] -> [Value] -> [Value]
updateRowIfNeeded updates maybeCondition columns row =
    case maybeCondition of
        Just condition -> if matchCondition condition columns row
                          then updateRow updates columns row
                          else row
        Nothing -> updateRow updates columns row

updateRow :: [(Lib2.Cname, Value)] -> [Column] -> [Value] -> [Value]
updateRow updates columns row =
    let updatesMap = M.fromList updates
    in zipWith (curry (\ (Column cname _, value) -> fromMaybe value (M.lookup cname updatesMap))) columns row

allColumnsExist :: [(Lib2.Cname, Value)] -> [Column] -> Bool
allColumnsExist updates columns =
    all (\(cname, _) -> any (\(Column colName _) -> cname == colName) columns) updates

allTypesMatch :: [(Lib2.Cname, Value)] -> [Column] -> Bool
allTypesMatch updates columns =
    all (\(cname, value) -> any (\(Column colName colType) -> cname == colName && valueMatchesType value colType) columns) updates

valueMatchesType :: Value -> ColumnType -> Bool
valueMatchesType (IntegerValue _) IntegerType = True
valueMatchesType (StringValue _) StringType = True
valueMatchesType (BoolValue _) BoolType = True
valueMatchesType NullValue _ = True
valueMatchesType _ _ = False
