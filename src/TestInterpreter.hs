{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}

module TestInterpreter
    ( runTestInterpreter
    , MockDatabase
    )
where

import Data.IORef
import Lib1
import Lib2
import Lib3
import Control.Monad.Free (Free (..))
import Data.Time.Clock (getCurrentTime, UTCTime (UTCTime))

type MockDatabase = [(Lib3.TableName, DataFrame)]

runTestInterpreter :: IORef MockDatabase -> Lib3.Execution r -> IO r
runTestInterpreter _ (Pure r) = return r
runTestInterpreter mockDb (Free step) = runStep mockDb step >>= runTestInterpreter mockDb

-- = LoadFile TableName (Either ErrorMessage FileContent -> next)
-- | SaveTableData TableName (Either ErrorMessage DataFrame) (Either ErrorMessage () -> next)
-- | GetTableNames ([TableName] -> next)
-- | GetTime (UTCTime -> next)
-- | CreateTable TableName [Column] (Either ErrorMessage DataFrame -> next)
-- | DropTable TableName (Either ErrorMessage () -> next)


runStep :: IORef MockDatabase -> Lib3.ExecutionAlgebra a -> IO a
runStep mockDb (Lib3.LoadFile tableName next) = do
    mockData <- readIORef mockDb
    let tableResult = findTableByName mockData tableName
    return $ next (Lib3.serializeDataFrame tableResult)
runStep _ (Lib3.GetTime next) =
    return $ next fixedTimestamp
runStep mockDb (Lib3.SaveTableData tableName newDf next) = do
    mockData <- readIORef mockDb
    let updatedMockData = updateTableInMockDB tableName newDf mockData
    writeIORef mockDb updatedMockData
    return $ next (Right ())
runStep mockDb (Lib3.GetTableNames next) = do
    mockData <- readIORef mockDb
    return $ next (map fst mockData)
runStep mockDb (Lib3.CreateTable tableName columns next) = do
    mockData <- readIORef mockDb
    let tableResult = findTableByName mockData tableName
    case tableResult of
        Left _ -> do
            let df = DataFrame columns []
            let newTable = (tableName, df)
            let updatedMockData = newTable : mockData
            writeIORef mockDb updatedMockData
            return $ next (Right df)
        Right _ -> return $ next (Left "Table already exists")
runStep mockDb (Lib3.DropTable tableName next) = do
    mockData <- readIORef mockDb
    let tableResult = findTableByName mockData tableName
    case tableResult of
        Left _ -> return $ next (Left "Table does not exist")
        Right _ -> do
            let updatedMockData = filter (\(tName, _) -> tName /= tableName) mockData
            writeIORef mockDb updatedMockData
            return $ next (Right ())


fixedTimestamp :: UTCTime
fixedTimestamp = read "2023-11-11 12:00:00 UTC"

updateTableInMockDB :: Lib3.TableName -> Either ErrorMessage DataFrame -> MockDatabase -> MockDatabase
updateTableInMockDB tableName newDf mockDb = case newDf of
    Left _ -> mockDb
    Right df -> map (\(tName, dbTable) -> if tName == tableName then (tName, df) else (tName, dbTable)) mockDb
