{-# LANGUAGE DeriveGeneric #-}
module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import GHC.Generics (Generic)
import Data.Aeson qualified as A
import Data.Aeson.Types qualified as AT (Parser, Value (Null))
import Data.Aeson.Key qualified as K (fromString)
import Control.Monad (forM)
import qualified Data.Text as T

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq, Generic, Read)

instance A.FromJSON ColumnType
instance A.ToJSON ColumnType

data Column = Column String ColumnType
  deriving (Show, Eq, Generic)

instance A.FromJSON Column
instance A.ToJSON Column

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | TimeValue String
  | NullValue
  deriving (Show, Eq, Generic)

instance A.FromJSON DataFrame.Value
instance A.ToJSON DataFrame.Value

type Row = [DataFrame.Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic)

instance A.ToJSON DataFrame where
    toJSON (DataFrame columns rows) =
        A.object [K.fromString "columns" A..= map columnToJSON columns,
                  K.fromString "rows" A..= map (rowToJSON columns) rows]
      where
        columnToJSON (Column name ctype) =
            A.object [K.fromString "name" A..= name, K.fromString "type" A..= show ctype]

        rowToJSON :: [Column] -> Row -> A.Value
        rowToJSON cols rowVals =
            A.object $ zipWith (\(Column name _) val -> K.fromString name A..= valueToJSON val) cols rowVals

        valueToJSON :: DataFrame.Value -> A.Value
        valueToJSON v = case v of
            IntegerValue i -> A.toJSON i
            StringValue s  -> A.toJSON s
            BoolValue b    -> A.toJSON b
            TimeValue t    -> A.toJSON t
            NullValue      -> AT.Null

instance A.FromJSON DataFrame where
    parseJSON = A.withObject "DataFrame" $ \v -> do
        columns <- v A..: K.fromString "columns" >>= mapM parseColumn
        rows <- (v A..:? K.fromString "rows" A..!= []) >>= mapM (parseRow columns)
        return $ DataFrame columns rows
      where
        parseColumn = A.withObject "Column" $ \v -> do
            name <- v A..: K.fromString "name"
            ctypeStr <- v A..: K.fromString "type"
            let ctype = read ctypeStr
            return $ Column name ctype

        parseRow cols = A.withObject "Row" $ \v -> forM cols $ \col -> do
            let (Column name ctype) = col
            val <- v A..: K.fromString name
            parseValue ctype val

parseValue :: ColumnType -> A.Value -> AT.Parser DataFrame.Value
parseValue ctype val = case (ctype, val) of
    (IntegerType, A.Number n) -> return $ IntegerValue (floor n)
    (StringType, A.String s)  -> return $ StringValue (T.unpack s)
    (BoolType, A.Bool b)      -> return $ BoolValue b
    (_, AT.Null)                 -> return NullValue
    _                         -> fail $ "Type mismatch or invalid value for " ++ show ctype
