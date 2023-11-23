{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# LANGUAGE OverloadedStrings #-}

module TestInterpreter
    ( runTestInterpreter
    , MockDatabase
    )
where

import Data.IORef
import Lib1
import Lib2
import Lib3
import Util
import Data.Time.Clock (getCurrentTime)
import DataFrame
import InMemoryTables
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

type MockDatabase = [(Lib3.TableName, DataFrame)]

runTestInterpreter :: IORef MockDatabase -> Lib3.Execution r -> IO r
runTestInterpreter mockDb (Pure r) = return r
runTestInterpreter mockDb (Free step) = runStep mockDb step >>= runTestInterpreter mockDb
    
runStep :: IORef MockDatabase -> Lib3.ExecutionAlgebra a -> IO a
runStep mockDb (Lib3.LoadFile tableName next) = do
    mockData <- readIORef mockDb
    let tableResult = findTableByName mockData tableName
    let serializedResult = Lib3.serializeDataFrame tableResult
    return $ next serializedResult
runStep _ (Lib3.ParseDataFrame fileContent next) =
    return $ next (Lib3.deserializeDataFrame fileContent)
runStep _ (Lib3.GetTime next) =
    do next <$> getCurrentTime
runStep mockDb (Lib3.SaveTableData tableName newDf next) = do
    mockData <- readIORef mockDb
    case newDf of
        Left m -> return $ next $ Left m
        Right df -> do
            let updatedMockData = updateTableInMockDB tableName df mockData
            writeIORef mockDb updatedMockData
            return $ next $ Right ()
runStep _ (Lib3.FilterRows maybeCondition dataframe next) = do
    return $ next $ Lib2.applyWhereClauses maybeCondition dataframe
runStep _ (Lib3.JoinTables maybeCondition df1 df2 next) = do
    return $ next $ joinTables maybeCondition df1 df2
runStep _ (Lib3.InsertInto values dataframe next) = do
    return $ next $ insertInto values dataframe
runStep _ (Lib3.AggregateData aggregates dataframe next) = do
    return $ next $ Lib2.applyAggregates aggregates dataframe
runStep _ (Lib3.SelectColumns aggregates dataframe next) = do
    return $ next $ Lib2.selectColumns aggregates dataframe
runStep _ (Lib3.UpdateTableDataFrame dataframe updates maybeCondition next) = do
    return$ next $ updateTableDataFrame dataframe updates maybeCondition
runStep _ (Lib3.ApplyTimeFunction columns time dataframe next) = do
    return $ next $ Lib2.applyTimeFunction columns time dataframe
runStep _ (Lib3.ReportError err next) = do
    return $ error ("shits broken: " ++ err)
runStep mockDb (Lib3.GetTableNames next) = do
    next . getTableNames <$> readIORef mockDb

updateTableInMockDB :: Lib3.TableName -> DataFrame -> MockDatabase -> MockDatabase
updateTableInMockDB tableName newDf = map updateTable
  where
    updateTable (tName, df) 
      | tName == tableName = (tName, newDf)
      | otherwise = (tName, df)


getTableNames :: MockDatabase -> [Lib3.TableName]
getTableNames = map fst
