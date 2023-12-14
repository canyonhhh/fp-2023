{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import Control.Monad.IO.Class (MonadIO (liftIO))
import GHC.Generics (Generic)
import Network.Wreq
import Control.Lens
import Data.Aeson (FromJSON(..), encode, eitherDecode, decode, withObject, (.:), (.:?))
import Control.Applicative ((<|>))
import qualified Data.ByteString.Lazy as BL
import qualified Lib1
import qualified Lib2
import DataFrame
import System.Console.Repline
import System.Console.Terminal.Size (Window, size, width)
import Data.List as L

type Repl a = HaskelineT IO a
type ErrorMessage = String

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
  result <- liftIO $ sendQuery input
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right df -> do
        liftIO $ putStrLn $ Lib1.renderDataFrameAsTable s df
  where
    terminalWidth :: Integral n => Maybe (Window n) -> n
    terminalWidth = maybe 80 width

data ServerResponse = ServerResponse
    { responseRight :: Maybe DataFrame
    , responseLeft :: Maybe String
    } deriving (Show)

instance FromJSON ServerResponse where
    parseJSON = withObject "ServerResponse" $ \v ->
        ServerResponse <$> v .:? "Right"
                       <*> v .:? "Left"

sendQuery :: String -> IO (Either ErrorMessage DataFrame)
sendQuery query = do
    let jsonPayload = encode query
    let headersList = [("Content-Type", "application/json")]
    httpResponse <- postWith (defaults & Network.Wreq.headers .~ headersList) "http://localhost:3000/query" jsonPayload
    let decodedResponse = eitherDecode (httpResponse ^. responseBody) :: Either String ServerResponse
    case decodedResponse of
        Left decodeErr -> return $ Left decodeErr
        Right serverResp ->
            case (responseRight serverResp, responseLeft serverResp) of
                (Just df, _) -> return $ Right df
                (_, Just errMsg) -> return $ Left errMsg
                _ -> return $ Left "Invalid server response"

-- Main function
main :: IO ()
main = evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final
