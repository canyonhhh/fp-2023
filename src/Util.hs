module Util 
    ( matchCondition
    , joinTables
    , insertInto
    , updateTableDataFrame
    ) where

import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.List qualified as L
import Lib2 qualified
import Lib3
import DataFrame

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
    if allColumnsExist updates columns then Right $ DataFrame columns (map (updateRowIfNeeded updates maybeCondition columns) rows)
    else Left "One or more columns not found."

updateRowIfNeeded :: [(Lib2.Cname, Value)] -> Maybe Lib2.Condition -> [Column] -> [Value] -> [Value]
updateRowIfNeeded updates maybeCondition columns row =
    case maybeCondition of
        Just condition -> if matchCondition condition columns row
                          then updateRow updates columns row
                          else row
        Nothing -> updateRow updates columns row

updateRow :: [(Lib2.Cname, Value)] -> [Column] -> [Value] -> [Value]
updateRow updates columns row =
    let updatesMap = Map.fromList updates
    in zipWith (curry (\ (Column cname _, value) -> fromMaybe value (Map.lookup cname updatesMap))) columns row

allColumnsExist :: [(Lib2.Cname, Value)] -> [Column] -> Bool
allColumnsExist updates columns =
    all (\(cname, _) -> any (\(Column colName _) -> cname == colName) columns) updates