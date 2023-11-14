{-# LANGUAGE DeriveGeneric #-}
module DataFrame (Column (..), ColumnType (..), DataFrame.Value (..), Row, DataFrame (..)) where

import GHC.Generics (Generic)
import Data.Aeson

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq, Generic)

instance ToJSON ColumnType
instance FromJSON ColumnType

data Column = Column String ColumnType
  deriving (Show, Eq, Generic)

instance ToJSON Column
instance FromJSON Column

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq, Generic)

instance ToJSON DataFrame.Value
instance FromJSON DataFrame.Value

type Row = [DataFrame.Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic)

instance ToJSON DataFrame
instance FromJSON DataFrame
