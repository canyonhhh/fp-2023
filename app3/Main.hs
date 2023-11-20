{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Time ( getCurrentTime)
import System.Directory ( doesFileExist, listDirectory )
import System.FilePath (takeBaseName, takeExtension)
import System.IO.Error (catchIOError)
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3
import DataFrame
import Util
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = runStep step >>= runExecuteIO

runStep :: Lib3.ExecutionAlgebra a -> IO a
runStep (Lib3.LoadFile tableName next) = do
    fileResult <- readFileIO tableName
    return $ next fileResult
runStep (Lib3.ParseDataFrame fileContent next) =
    return $ next (Lib3.deserializeDataFrame fileContent)
runStep (Lib3.GetTime next) =
    do next <$> getCurrentTime
runStep (Lib3.SaveTableData tableName dataframe next) = 
    case Lib3.serializeDataFrame dataframe of
        Left errMsg -> return $ next $ Left errMsg
        Right serializedData -> do
            writeFileIO tableName serializedData
            return $ next $ Right ()
runStep (Lib3.FilterRows maybeCondition dataframe next) = do
    return $ next $ Lib2.applyWhereClauses maybeCondition dataframe
runStep (Lib3.JoinTables maybeCondition df1 df2 next) = do
    return $ next $ joinTables maybeCondition df1 df2
runStep (Lib3.InsertInto values dataframe next) = do
    return $ next $ insertInto values dataframe
runStep (Lib3.AggregateData aggregates dataframe next) = do
    return $ next $ Lib2.applyAggregates aggregates dataframe
runStep (Lib3.SelectColumns aggregates dataframe next) = do
    return $ next $ Lib2.selectColumns aggregates dataframe
runStep (Lib3.UpdateTableDataFrame dataframe updates maybeCondition next) = do
    return$ next $ updateTableDataFrame dataframe updates maybeCondition
runStep (Lib3.ReportError err next) = do
    return $ error ("shits broken: " ++ err)
runStep (Lib3.GetTableNames next) = do
    next <$> getTableNames

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
