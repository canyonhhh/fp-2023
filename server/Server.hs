{-# LANGUAGE OverloadedStrings #-}

module Main where

import Web.Scotty
import qualified Data.Text.Lazy as T
import DataFrame
import Data.Aeson (FromJSON, eitherDecode, encode)
import qualified Data.ByteString.Lazy as BL
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

main :: IO ()
main = scotty 3000 $ do
    get "/health" $ do
        text $ T.pack "Server is up and running"

    post "/query" $ do
        body <- body
        let parsed = eitherDecode body :: Either String ParsedStatement
        case parsed of
            Left err -> text $ T.pack err
            Right stmt -> do
                liftIO $ print stmt
                result <- liftIO $ runExecuteIO $ Lib3.executeSql stmt
                json $ encode result

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = runStep step >>= runExecuteIO

runStep :: Lib3.ExecutionAlgebra a -> IO a
runStep (Lib3.LoadFile tableName next) = do
    fileResult <- readFileIO tableName
    return $ next fileResult
runStep (Lib3.GetTime next) =
    do next <$> getCurrentTime
runStep (Lib3.SaveTableData tableName dataframe next) = 
    case Lib3.serializeDataFrame dataframe of
        Left errMsg -> return $ next $ Left errMsg
        Right serializedData -> do
            writeFileIO tableName serializedData
            return $ next $ Right ()
runStep (Lib3.GetTableNames next) = 
    next <$> getTableNames
runStep (Lib3.DropTable tableName next) = do
    fileExists <- doesFileExist $ "db/" ++ tableName ++ ".yaml"
    if fileExists
        then do
            removeFile $ "db/" ++ tableName ++ ".yaml"
            return $ next $ Right ()
        else return $ next $ Left $ "Table " ++ tableName ++ " does not exist."
runStep (Lib3.CreateTable tableName colHeader next) = do
    fileExists <- doesFileExist $ "db/" ++ tableName ++ ".yaml"
    if fileExists
        then return $ next $ Left $ "Table " ++ tableName ++ " already exists."
        else do
            let fileContent = Lib3.serializeDataFrame $ DataFrame colHeader []
            writeFileIO tableName fileContent
            return $ next $ Right ()

reportError :: String -> DataFrame
reportError err = DataFrame [Column "error" StringType] [[StringValue err]]

-- check if file exists
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
