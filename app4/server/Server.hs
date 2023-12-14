{-# LANGUAGE OverloadedStrings #-}

module Main where

import Web.Scotty
import qualified Data.Text.Lazy as T
import qualified Data.ByteString.Lazy as BL
import DataFrame
import Data.Aeson (FromJSON, eitherDecode, encode, decode)
import System.Directory ( doesFileExist, listDirectory, removeFile )
import System.IO (readFile, writeFile)
import Data.List (isSuffixOf)
import Data.Maybe (fromMaybe)
import Data.Either (fromRight)
import System.FilePath (takeBaseName, takeExtension)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Free (Free (..))
import Data.Time.Clock (getCurrentTime)
import System.IO.Error (catchIOError)
import Lib1
import Lib2
import Lib3
import System.FilePath ((</>), takeBaseName, takeExtension)
import qualified Data.Yaml as Yaml
import Control.Concurrent.STM.TVar 
import qualified Data.Map as Map
import Control.Monad (forever)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM (atomically, readTVar)
import Control.Monad.IO.Class (liftIO)

loadTables :: FilePath -> IO (Map.Map String DataFrame)
loadTables dbPath = do
    files <- listDirectory dbPath
    let yamlFiles = filter ((== ".yaml") . takeExtension) files
    tables <- mapM loadTable yamlFiles
    return $ Map.fromList tables
  where
    loadTable file = do
        let tableName = takeBaseName file
        content <- Yaml.decodeFileEither (dbPath </> file)
        case content of
            Left err -> error $ "Error loading table " ++ tableName ++ ": " ++ show err
            Right df -> return (tableName, df)

type Tables = Map.Map String DataFrame

initTables :: Map.Map String DataFrame -> IO (TVar Tables)
initTables tables = atomically $ newTVar tables

saveTablesPeriodically :: FilePath -> TVar Tables -> IO ()
saveTablesPeriodically dbPath tablesVar = do
    _ <- forkIO $ forever $ do
        threadDelay 1000000
        currentTables <- atomically $ readTVar tablesVar
        mapM_ (\(tableName, df) -> Yaml.encodeFile (dbPath </> tableName ++ ".yaml") df) 
              (Map.toList currentTables)
    return ()

main :: IO ()
main = do
    let dbPath = "db"
    tablesMap <- loadTables dbPath
    tablesVar <- initTables tablesMap
    _ <- saveTablesPeriodically dbPath tablesVar

    scotty 3000 $ do
        post "/query" $ do
            requestBody <- body
            let parsed = eitherDecode requestBody :: Either ErrorMessage String
            case parsed of
                Left errMsg -> json $ errMsg
                Right query -> do
                    executionResult <- liftIO $ runExecuteIO tablesVar $ Lib3.executeSql query
                    liftIO $ print executionResult
                    json executionResult

runExecuteIO :: TVar Tables -> Lib3.Execution r -> IO r
runExecuteIO tablesVar (Pure r) = return r
runExecuteIO tablesVar (Free step) = runStep tablesVar step >>= runExecuteIO tablesVar

runStep :: TVar Tables -> Lib3.ExecutionAlgebra a -> IO a
runStep tablesVar (Lib3.LoadFile tableName next) = do
    tables <- atomically $ readTVar tablesVar
    let fileResult = case Map.lookup tableName tables of
                        Just df -> case serializeDataFrame (Right df) of
                            Right content -> Right content
                            Left errMsg -> Left errMsg
                        Nothing -> Left $ "Table " ++ tableName ++ " does not exist."
    return $ next fileResult
runStep tablesVar (Lib3.SaveTableData tableName dataframeResult next) = do
    case dataframeResult of
        Left errMsg -> return $ next $ Left errMsg
        Right dataframe -> do
            atomically $ modifyTVar' tablesVar (Map.insert tableName dataframe)
            return $ next $ Right ()
runStep tablesVar (Lib3.GetTableNames next) = do
    tableNames <- atomically $ Map.keys <$> readTVar tablesVar
    return $ next tableNames
runStep tablesVar (Lib3.DropTable tableName next) = do
    atomically $ modifyTVar' tablesVar (Map.delete tableName)
    -- also delete the file
    let fileName = "db/" ++ tableName ++ ".yaml"
    fileExists <- doesFileExist fileName
    if fileExists
        then removeFile fileName
        else return ()
    return $ next $ Right ()
runStep tablesVar (Lib3.CreateTable tableName colHeader next) = do
    tables <- atomically $ readTVar tablesVar
    if Map.member tableName tables
        then return $ next $ Left $ "Table " ++ tableName ++ " already exists."
        else do
            let newTable = DataFrame colHeader []
            atomically $ modifyTVar' tablesVar (Map.insert tableName newTable)
            return $ next $ Right newTable
runStep tablesVar (Lib3.GetTime next) =
    do currentTime <- getCurrentTime
       return $ next currentTime

readFileIO :: TableName -> IO (Either ErrorMessage FileContent)
readFileIO tableName = do
    let fileName = "db/" ++ tableName ++ ".yaml"
    fileExists <- doesFileExist fileName
    if fileExists
        then Right <$> readFile fileName
        else return $ Left $ "Table " ++ tableName ++ " does not exist."

writeFileIO :: TableName -> FileContent -> IO ()
writeFileIO tableName fileContent = do
    let fileName = "db/" ++ tableName ++ ".yaml"
    writeFile fileName fileContent

getTableNames :: IO [TableName]
getTableNames = catchIOError listTableNames handleIOError
  where
    listTableNames = do
      files <- listDirectory "db"
      let yamlFiles = filter (\f -> takeExtension f == ".yaml") files
      return $ map takeBaseName yamlFiles

    handleIOError :: IOError -> IO [TableName]
    handleIOError _ = return []
