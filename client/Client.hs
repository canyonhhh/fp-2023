{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Network.Wreq
import Control.Lens
import Data.Aeson (encode, eitherDecode)
import qualified Data.ByteString.Lazy as BL
import qualified Lib1
import qualified Lib2
import DataFrame
import System.Console.Repline
import System.Console.Terminal.Size (Window, size, width)
import Data.List as L

type Repl a = HaskelineT IO a

-- Initialize REPL
ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to the database client! Press [TAB] for auto completion."

-- Finalize REPL
final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

-- Command completer
completer :: Monad m => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete", "drop", "create"
              ]
  return $ filter (L.isPrefixOf n) names

-- Handle user input
cmd :: String -> Repl ()
cmd input = do
  s <- terminalWidth <$> liftIO size
  case Lib2.parseStatement input of
    Left err -> liftIO $ putStrLn $ "Parse Error: " ++ err
    Right parsedQuery -> do
      result <- liftIO $ sendQuery parsedQuery
      case result of
        Left err -> liftIO $ putStrLn $ "Error: " ++ err
        Right df -> liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
  where
    terminalWidth :: Integral n => Maybe (Window n) -> n
    terminalWidth = maybe 80 width

-- Send query to server and get response
sendQuery :: Lib2.ParsedStatement -> IO (Either String DataFrame)
sendQuery query = do
    let jsonPayload = encode query
    let headersList = [("Content-Type", "application/json")]
    response <- postWith (defaults & Network.Wreq.headers .~ headersList) "http://localhost:3000/query" jsonPayload
    return $ eitherDecode (response ^. responseBody)

-- Main function
main :: IO ()
main = evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

