{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
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
import Data.Char (toUpper, isLetter, isSpace, isAlphaNum)
import Data.List (isPrefixOf, elemIndex, partition, transpose)
import Control.Applicative (many, some, Alternative(empty, (<|>)), optional)

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
    | SelectAll TableName
    | Select { columns :: [(Aggregate, String)]
             , tableName :: TableName
             , whereClause :: Maybe Condition}
    deriving (Show, Eq)

-- Parser

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

instance Alternative Parser where
    empty = Parser $ \_ -> Left "Error"
    p1 <|> p2 = Parser $ \inp ->
        case runParser p1 inp of
            Right a1 -> Right a1
            Left _ -> case runParser p2 inp of
                Right a2 -> Right a2
                Left err -> Left err

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

-- Where Clause Parsing

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
    cname <- some (satisfy isAlphaNum)
    _ <- optional whitespace
    _ <- keyword "IS"
    _ <- optional whitespace
    value <- some (satisfy isLetter)
    case map toUpper value of
        "TRUE" -> return (cname, True)
        "FALSE" -> return (cname, False)
        _ -> parserFail "Expected boolean value"

-- Column and Aggregate Parsing

aggregateParser :: Parser (Aggregate, String)
aggregateParser = do
    aggregate <- keyword "SUM(" <|> keyword "MAX(" <|> pure ""
    cname <- some (satisfy isAlphaNum)
    _ <- case aggregate of
        "" -> pure ""
        _ -> keyword ")"
    return (case aggregate of
        "SUM(" -> Sum
        "MAX(" -> Max
        _ -> None, cname)

columnsParser :: Parser [(Aggregate, String)]
columnsParser = some (aggregateParser <* optional (keyword "," <* optional whitespace))

-- Main parsers

selectAllParser :: Parser ParsedStatement
selectAllParser = do
    _ <- keyword "SELECT"
    _ <- whitespace
    _ <- keyword "*"
    _ <- whitespace
    _ <- keyword "FROM"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- optional whitespace
    _ <- keyword ";"
    return $ SelectAll tableName

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
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- optional whitespace
    _ <- keyword ";"
    return $ ShowTable tableName

selectParser :: Parser ParsedStatement
selectParser = do
    _ <- keyword "SELECT"
    _ <- whitespace
    agr <- columnsParser
    _ <- whitespace
    _ <- keyword "FROM"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
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

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement = runParser (try selectParser <|> try showTablesParser <|> try showTableParser <|> try selectAllParser) statement >>= \case
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
executeStatement (SelectAll name) = maybe (Left "Table not found") Right (findTableByName InMemoryTables.database name)
executeStatement (Select columns tableName whereClause) = 
    applyAggregates columns . selectColumns columns . applyWhereClauses whereClause . findTableByName InMemoryTables.database $ tableName

-- SHOW TABLES and SHOW TABLE
showTables :: Database -> Either ErrorMessage DataFrame
showTables db = Right $ DataFrame [Column "Tables_in_database" StringType] [[StringValue a] | (a, _) <- db]

-- finds table by name and returns the columns and data types of the table (without the rows)
showTable :: Database -> TableName -> Either ErrorMessage DataFrame
showTable db tableName = case findTableByName db tableName of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "Field" StringType, Column "Type" StringType] [[StringValue cname, StringValue (show ctype)] | Column cname ctype <- columns]
    Nothing -> Left "Table not found"

-- Where clauses

applyWhereClauses :: Maybe Condition -> Maybe DataFrame -> Either ErrorMessage DataFrame
applyWhereClauses _ Nothing = Left "Table not found"
applyWhereClauses Nothing (Just df) = Right df
applyWhereClauses _ (Just (DataFrame [] [])) = Left "Empty table"
applyWhereClauses (Just condition) (Just (DataFrame columns rows)) = 
    let 
        typesValid = verifyWhereClauseTypes columns condition
    in
        if not typesValid then Left "Cannot apply WHERE clause to non-boolean column"
        else Right (DataFrame columns (filter (applyCondition columns condition) rows))
 
applyCondition :: [Column] -> Condition -> [Value] -> Bool
applyCondition columns (And c1 c2) values = applyCondition columns c1 values && applyCondition columns c2 values
applyCondition columns (BoolCondition cname bool) values =
    let columnIdx = getIndex cname columns
        value = values !! columnIdx
    in case value of
        (BoolValue b) -> b == bool
        NullValue -> False
        _ -> error "Non-boolean condition not implemented"
 
 
verifyWhereClauseTypes :: [Column] -> Condition -> Bool 
verifyWhereClauseTypes columns (And c1 c2) = verifyWhereClauseTypes columns c1 && verifyWhereClauseTypes columns c2
verifyWhereClauseTypes columns (BoolCondition cname _) =
    case [(ctype, cname) | Column cname' ctype <- columns, cname == cname'] of
        ((BoolType, _):_) -> True
        _ -> False

-- Aggregates

applyAggregates :: [(Aggregate, String)] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyAggregates _ (Left m) = Left m
applyAggregates _ (Right (DataFrame [] [])) = Left "Empty table"
applyAggregates criteria (Right (DataFrame columns rows)) =
    let (nones, notNones) = partition (== None) [aggregate | (aggregate, _) <- criteria]
        validAggregates = verifyAggregates (nones, notNones)
        validTypes = verifyAggregateTypes columns criteria
        columns' = [Column (show aggregate ++ "("++cname++")") ctype | (aggregate, cname) <- criteria, Column cname' ctype <- columns, cname == cname']
        valueGroups = transpose rows
        applicator = [if aggregate == Max then applyMaxAggregate else applySumAggregate | (aggregate, _) <- criteria]
        rows' = transpose $ zipWith (\f values -> [f values]) applicator valueGroups
    in
        if null notNones then Right (DataFrame columns rows)
        else if not validAggregates then Left "Cannot mix aggregate and non-aggregate functions"
        else if not validTypes then Left "Cannot apply SUM() to non-integer column"
        else Right (DataFrame columns' rows')

applyMaxAggregate :: [Value] -> Value
applyMaxAggregate v = case head values of
    (IntegerValue _) -> IntegerValue $ maximum [x | IntegerValue x <- values]
    (BoolValue _) -> BoolValue $ maximum [x | BoolValue x <- values]
    (StringValue _) -> StringValue $ maximum [x | StringValue x <- values]
    NullValue -> NullValue
    where
        values = filter (/= NullValue) v

applySumAggregate :: [Value] -> Value
applySumAggregate values = case head values of
    (IntegerValue _) -> IntegerValue $ sum [x | IntegerValue x <- values]
    NullValue -> NullValue
    (BoolValue _) -> error "Cannot apply SUM() to Bool column (Invalid path)"
    (StringValue _) -> error "Cannot apply SUM() to String column (Invalid path)"

verifyAggregateTypes :: [Column] -> [(Aggregate, String)] -> Bool
verifyAggregateTypes columns criteria =
    let aggregates = [(ctype, aggregate) | (Column cname ctype) <- columns, (aggregate, cname') <- criteria, cname == cname']
        sums = filter (\(_, aggregate) -> aggregate == Sum) aggregates
        -- MAX() works with every datatype
        --maxs = filter (\(ctype, aggregate) -> aggregate == Max) aggregates 
        invalidSums = any (\(ctype, _) -> ctype /= IntegerType) sums 
    in  not invalidSums

verifyAggregates :: ([Aggregate], [Aggregate]) -> Bool
verifyAggregates (nones, notNones)
  | not (null notNones) && not (null nones) = False
  | otherwise = True

-- List Columns

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
