{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    Aggregate (..),
    Database,
    showTables,
    showTable,
    applyWhereClauses,
    applyAggregates,
    selectColumns
  )
where

import DataFrame
import Lib1
import InMemoryTables (TableName, database)
import Data.Char (toUpper, isLetter, isSpace)
import Data.List (isPrefixOf, isSuffixOf, elemIndex)
import Control.Monad
import Control.Monad.Fail

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Condition
    = And Condition Condition
    | BoolCondition String Bool
    deriving (Show, Eq)

data Aggregate
    = Max 
    | Sum 
    | None
    deriving (Show, Eq)

data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | Select { columns :: [(Aggregate, String)]
             , tableName :: TableName
             , whereClause :: Maybe Condition}
    deriving (Show, Eq)

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
  fmap f functor = Parser $ \inp ->
    case runParser functor inp of
        Left e -> Left e
        Right (l, a) -> Right (l, f a)

instance Applicative Parser where
  pure a = Parser $ \inp -> Right (inp, a)
  ff <*> fa = Parser $ \in1 ->
    case runParser ff in1 of
        Left e1 -> Left e1
        Right (in2, f) -> case runParser fa in2 of
            Left e2 -> Left e2
            Right (in3, a) -> Right (in3, f a)

instance Monad Parser where
  ma >>= mf = Parser $ \inp1 ->
    case runParser ma inp1 of
        Left e1 -> Left e1
        Right (inp2, a) -> case runParser (mf a ) inp2 of
            Left e2 -> Left e2
            Right (inp3, r) -> Right (inp3, r)

(<|>) :: Parser a -> Parser a -> Parser a
pa <|> pb = Parser $ \inp ->
    case runParser pa inp of
        Left _ -> runParser pb inp
        r -> r

parserFail :: ErrorMessage -> Parser a
parserFail msg = Parser $ \_ -> Left msg

keyword :: String -> Parser String
keyword kw = Parser $ \inp ->
    if map toUpper kw `isPrefixOf` map toUpper inp
    then Right (drop (length kw) inp, kw)
    else Left ("Expected keyword " ++ kw)

try :: Parser a -> Parser a
try p = Parser $ \inp ->
    case runParser p inp of
        Left _ -> Left inp
        result -> result

optional :: Parser a -> Parser (Maybe a)
optional p = (Just <$> p) <|> pure Nothing

some :: Parser a -> Parser [a]
some p = (:) <$> p <*> many p

many :: Parser a -> Parser [a]
many p = some p <|> pure []

satisfy :: (Char -> Bool) -> Parser Char
satisfy f = Parser $ \case
        [] -> Left "Unexpected end of input"
        (x:xs) -> if f x
                  then Right (xs, x)
                  else Left ("Unexpected character " ++ [x])

whitespace :: Parser String
whitespace = Parser $ \inp ->
    let (whitespaces, rest) = span isSpace inp
    in case whitespaces of
       [] -> Left "Expected whitespace"
       _ -> Right (rest, whitespaces)

selectParser :: Parser ParsedStatement
selectParser = do
    _ <- keyword "SELECT"
    _ <- whitespace
    agr <- columnsParser
    _ <- whitespace
    _ <- keyword "FROM"
    _ <- whitespace
    tableName <- some (satisfy isLetter)
    do 
        _ <- optional whitespace
        _ <- keyword ";"
        return $ Select agr tableName Nothing
        <|> do
           _ <- whitespace
           _ <- keyword "WHERE"
           whereClause <- Just <$> whereParser <|> parserFail "Malformed where clause"
           _ <- optional whitespace
           _ <- keyword ";"
           return $ Select agr tableName whereClause

whereParser :: Parser Condition
whereParser = do
    firstCondition <- conditionParser
    restConditions <- many (whitespace *> keyword "AND" *> conditionParser)
    parseCondition (firstCondition:restConditions)

-- Recursively parse a list of tuples [(cname, True/False), ...] into a Condition
parseCondition :: [(String, Bool)] -> Parser Condition
parseCondition [] = parserFail "Empty condition"
parseCondition [(cname, bool)] = return $ BoolCondition cname bool
parseCondition ((cname, bool):xs) = do
    condition <- parseCondition xs
    return $ And (BoolCondition cname bool) condition

conditionParser :: Parser (String, Bool)
conditionParser = do
    _ <- whitespace
    cname <- some (satisfy isLetter)
    _ <- optional whitespace
    _ <- keyword "IS"
    _ <- optional whitespace
    value <- some (satisfy isLetter)
    case map toUpper value of
        "TRUE" -> return (cname, True)
        "FALSE" -> return (cname, False)
        _ -> parserFail "Expected boolean value"

aggregateParser :: Parser (Aggregate, String)
aggregateParser = do
    aggregate <- keyword "SUM(" <|> keyword "MAX(" <|> pure ""
    cname <- some (satisfy isLetter)
    _ <- case aggregate of
        "" -> pure ""
        _ -> keyword ")"
    return (case aggregate of
        "SUM(" -> Sum
        "MAX(" -> Max
        _ -> None, cname)

showTablesParser :: Parser ParsedStatement
showTablesParser  = do
    _ <- keyword "SHOW"
    _ <- whitespace
    _ <- keyword "TABLES"
    _ <- optional whitespace
    _ <- keyword ";"
    return ShowTables

showTableParser :: Parser ParsedStatement
showTableParser = do
    _ <- keyword "SHOW"
    _ <- whitespace
    _ <- keyword "TABLE"
    _ <- whitespace
    tableName <- some (satisfy isLetter) <|> parserFail "Expected table name"
    --when (null tableName) (parserFail "Expected table name")
    _ <- optional whitespace
    _ <- keyword ";"
    return $ ShowTable tableName

columnsParser :: Parser [(Aggregate, String)]
columnsParser = some (aggregateParser <* optional (keyword "," <* optional whitespace))

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement = runParser (try selectParser <|> showTablesParser <|> showTableParser) statement >>= \case
    ("", parsedStatement) -> Right parsedStatement
    (rest, _) -> Left ("Unexpected input: " ++ rest)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- First the where clause is applied to the specified columns
-- Then the aggregate functions are applied to the columns specified in the select statement
-- The columns are then filtered to only include the columns specified in the select statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = showTables InMemoryTables.database
executeStatement (ShowTable name) = showTable InMemoryTables.database name
executeStatement (Select columns tableName whereClause) = 
    selectColumns columns . applyAggregates columns . applyWhereClauses whereClause . findTableByName InMemoryTables.database $ tableName

showTables :: Database -> Either ErrorMessage DataFrame
showTables db = Right $ DataFrame [Column "Tables_in_database" StringType] [[StringValue a] | (a, _) <- db]

showTable :: Database -> TableName -> Either ErrorMessage DataFrame
showTable db tableName = case findTableByName db tableName of
    Just df -> Right df
    Nothing -> Left "Table not found"

applyWhereClauses :: Maybe Condition -> Maybe DataFrame -> Either ErrorMessage DataFrame
applyWhereClauses _ Nothing = Left "Table not found"
applyWhereClauses _ _ = Left "Not implemented"

applyAggregates :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyAggregates _ (Left m) = Left m
applyAggregates _ _ = Left "Not implemented"


selectColumns :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
selectColumns _ (Left m) = Left m
selectColumns criteria (Right (DataFrame columns rows))
  | null columns && null rows = Left "Empty table"
  | otherwise =
    let validColumns = filter (\(_, colName) -> any (\(Column name _) -> colName == name) columns) criteria
        validColumnIndices = map (\(_, colName) -> getIndex colName columns) validColumns
        missingColumns = filter (\(_, colName) -> colName `notElem` map (\(Column name _) -> name) columns) criteria
        errorMessage = if not (null missingColumns)
                       then "Column " ++ snd (head missingColumns) ++ " not found"
                       else ""
        filteredColumns = [columns !! idx | idx <- validColumnIndices]
        filteredRows = map (\row -> [row !! idx | idx <- validColumnIndices]) rows
    in if null errorMessage
       then Right (DataFrame filteredColumns filteredRows)
       else Left errorMessage

getIndex :: String -> [Column] -> Int
getIndex name cols = case  name  `elemIndex` [cname | Column cname _ <- cols] of
  Just idx -> idx
  Nothing  -> error ("Column " ++ name ++ " not found")
