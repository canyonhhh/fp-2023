{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    Aggregate (..),
    Database,
    showTable,
    applyWhereClauses,
    applyAggregates,
    selectColumns,
    applyTimeFunction,
    Cname,
    Value (..),
    Column (..),
    DataFrame (..),
    applyOrder,
    Order (..),
    OrderBy (..),
    ColumnExpression (..)
  )
where

import DataFrame
import Data.Char (toUpper, isSpace, isAlphaNum, isDigit, isAscii)
import Data.List (isPrefixOf, elemIndex, partition, transpose, find, findIndex, sortBy)
import Control.Applicative (many, some, Alternative, Alternative(empty, (<|>)), optional)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Time(UTCTime)
import Data.Function (on)
import GHC.Generics (Generic)
import Data.Aeson qualified as A
import Control.Monad.State
import Control.Monad.Except
import Control.Monad.Identity

type ErrorMessage = String
type Cname = String
type TableName = String
type Database = [(TableName, DataFrame)]

data ColumnExpression
    = SimpleColumn Cname
    | AggregateColumn Aggregate Cname
    | FunctionCall String
    deriving (Show, Eq, Generic)

instance A.FromJSON ColumnExpression
instance A.ToJSON ColumnExpression

data Condition
    = And Condition Condition
    | Equal Cname Value
    | BoolCondition Cname Bool
    | JoinCondition Cname Cname
    deriving (Show, Eq, Generic)

instance A.FromJSON Condition
instance A.ToJSON Condition

data Aggregate
    = Max 
    | Sum 
    | None
    deriving (Show, Eq, Generic)

instance A.FromJSON Aggregate
instance A.ToJSON Aggregate

data Order
    = Asc
    | Desc
    deriving (Show, Eq, Generic)

instance A.FromJSON Order
instance A.ToJSON Order

data OrderBy
    = OrderBy Cname Order
    deriving (Show, Eq, Generic)

instance A.FromJSON OrderBy
instance A.ToJSON OrderBy

data ParsedStatement
    = ShowTables
    | ShowTable TableName
    | ShowTime
    | SelectAll { tableName :: TableName 
                , whereClause :: Maybe Condition
                , orderBy :: Maybe [OrderBy]}
    | Select { columns :: [ColumnExpression]
             , tableNames :: [TableName]
             , whereClause :: Maybe Condition
             , orderBy :: Maybe [OrderBy]}
    | Insert { tableName :: TableName
             , values :: [(Cname, Value)]}
    | Delete { tableName :: TableName
             , whereClause :: Maybe Condition}
    | Update { tableName :: TableName
             , values :: [(Cname, Value)]
             , whereClause :: Maybe Condition}
    | Drop   { tableName :: TableName}
    | Create { tableName :: TableName
             , colHeader :: [Column]}
    deriving (Show, Eq, Generic)

instance A.FromJSON ParsedStatement
instance A.ToJSON ParsedStatement

-- Parser

newtype Parser a = Parser { runParser :: ExceptT String (State String) a }

instance MonadError String Parser where
    throwError = Parser . throwError
    catchError (Parser p) handler = Parser $ catchError p (runParser . handler)

instance MonadState String Parser where
    get = Parser get
    put = Parser . put

instance Functor Parser where
    fmap f (Parser p) = Parser (fmap f p)

instance Applicative Parser where
    pure = Parser . pure
    (Parser f) <*> (Parser a) = Parser (f <*> a)

instance Monad Parser where
    (Parser a) >>= f = Parser $ a >>= runParser . f

instance Alternative Parser where
    empty = Parser empty
    (Parser p1) <|> (Parser p2) = Parser $ ExceptT $ StateT $ \s -> do
        let res = runStateT (runExceptT p1) s
        case runIdentity res of
            (Left _, _) -> runStateT (runExceptT p2) s
            (Right val, s') -> return (Right val, s')

parserFail :: String -> Parser a
parserFail msg = throwError msg

keyword :: String -> Parser String
keyword kw = do
    input <- get
    let kwUpper = map toUpper kw
        inputUpper = map toUpper input
    if kwUpper `isPrefixOf` inputUpper
        then put (drop (length kw) input) >> return kw
        else throwError $ "Expected keyword " ++ kw

satisfy :: (Char -> Bool) -> Parser Char
satisfy f = do
    input <- get
    case input of
        (x:xs) -> if f x
                  then put xs >> return x
                  else throwError $ "Unexpected character " ++ [x]
        [] -> throwError "Unexpected end of input"

whitespace :: Parser String
whitespace = do
    input <- get
    let (spaces, rest) = span isSpace input
    case spaces of
        [] -> throwError "Expected whitespace"
        _ -> put rest >> return spaces

-- Where Clause Parsing
whereParser :: Parser Condition
whereParser = do
    firstCondition <- conditionParser
    restConditions <- many (optional whitespace *> keyword "AND" *> conditionParser)
    return $ foldl And firstCondition restConditions

conditionParser :: Parser Condition
conditionParser = do
    _ <- whitespace
    cname <- some alphaNumOrUnderscore
    _ <- optional whitespace
    (equalCondition cname) <|> (boolCondition cname) <|> joinConditionParser cname

joinConditionParser :: Cname -> Parser Condition
joinConditionParser cname1= do
    _ <- optional whitespace
    _ <- keyword "="
    _ <- optional whitespace
    cname2 <- some alphaNumOrUnderscore
    _ <- optional whitespace
    return $ JoinCondition cname1 cname2

equalCondition :: Cname -> Parser Condition
equalCondition columnName = do
   _ <- keyword "="
   _ <- optional whitespace
   Equal columnName <$> valueParser

boolCondition :: Cname -> Parser Condition
boolCondition columnName = do
    _ <- keyword "IS"
    _ <- optional whitespace
    value <- (keyword "TRUE" *> pure True) <|> (keyword "FALSE" *> pure False)
    return $ BoolCondition columnName value

-- Column and Aggregate Parsing
aggregateParser :: Parser ColumnExpression
aggregateParser = do
    aggregate <- keyword "SUM(" <|> keyword "MAX(" <|> keyword "NOW()" <|> pure ""
    if aggregate == "NOW()"
    then return $ FunctionCall "NOW()"
    else do
        cname <- some alphaNumOrUnderscore
        case aggregate of
            "" -> return $ SimpleColumn cname
            _ -> do
                _ <- keyword ")"
                return $ AggregateColumn (parseAggregate aggregate) cname

parseAggregate :: String -> Aggregate
parseAggregate agg = case agg of
    "SUM(" -> Sum
    "MAX(" -> Max
    _ -> error "Invalid path"

columnsParser :: Parser [ColumnExpression]
columnsParser = some (aggregateParser <* optional (optional whitespace <* keyword "," <* optional whitespace))

-- Value Parsing
valueParser :: Parser Value
valueParser = do
    _ <- optional whitespace
    value <- boolParser <|> intParser <|> stringParser <|> nullParser <|> parserFail "Invalid value"
    _ <- optional whitespace
    _ <- optional $ keyword ","
    _ <- optional whitespace
    return value

boolParser :: Parser Value
boolParser = do
    value <- keyword "TRUE" <|> keyword "FALSE"
    case value of
        "TRUE"  -> return $ BoolValue True
        "FALSE" -> return $ BoolValue False
        _       -> throwError "Invalid boolean value"

intParser :: Parser Value
intParser = do
    digits <- some (satisfy isDigit)
    return $ IntegerValue (read digits)

stringParser :: Parser Value
stringParser = do
    _ <- keyword "'"
    string <- some (satisfy (\c -> isAscii c && c /= '\''))
    _ <- keyword "'"
    return $ StringValue string

nullParser :: Parser Value
nullParser = do
    _ <- keyword "NULL"
    return NullValue

orderParser :: Parser [OrderBy]
orderParser = do
    _ <- whitespace
    _ <- keyword "ORDER"
    _ <- whitespace
    _ <- keyword "BY"
    _ <- whitespace
    firstOrder <- orderByParser
    restOrders <- many (optional whitespace *> keyword "," *> orderByParser)
    return $ firstOrder : restOrders

orderByParser :: Parser OrderBy
orderByParser = do
    _ <- optional whitespace
    cname <- some alphaNumOrUnderscore
    _ <- optional whitespace
    order <- (keyword "ASC" *> pure Asc) <|> (keyword "DESC" *> pure Desc) <|> pure Asc
    return $ OrderBy cname order

-- Main parsers
selectAllParser :: Parser ParsedStatement
selectAllParser = do
    _ <- keyword "SELECT"
    _ <- whitespace
    _ <- keyword "*"
    _ <- whitespace
    _ <- keyword "FROM"
    _ <- whitespace
    tableName <- some alphaNumOrUnderscore
    do
        order <- optional orderParser
        _ <- optional whitespace
        _ <- keyword ";"
        return $ SelectAll tableName Nothing order
        <|> do
           _ <- whitespace
           _ <- keyword "WHERE"
           whereClause <- Just <$> whereParser <|> throwError "Malformed where clause"
           order <- optional orderParser
           _ <- optional whitespace
           _ <- keyword ";"
           return $ SelectAll tableName whereClause order

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
    tableName <- some alphaNumOrUnderscore
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
    tableNames <- some (some alphaNumOrUnderscore <* optional (optional whitespace <* keyword "," <* optional whitespace))
    do 
        order <- optional orderParser
        _ <- optional whitespace
        _ <- keyword ";"
        return $ Select agr tableNames Nothing order
        <|> do
           _ <- whitespace
           _ <- keyword "WHERE"
           whereClause <- Just <$> whereParser <|> parserFail "Malformed where clause"
           order <- optional orderParser
           _ <- optional whitespace
           _ <- keyword ";"
           return $ Select agr tableNames whereClause order

-- WRITING OPERATION PARSERS
insertParser :: Parser ParsedStatement
insertParser = do
    _ <- keyword "INSERT"
    _ <- whitespace
    _ <- keyword "INTO"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- whitespace
    _ <- keyword "("
    _ <- optional whitespace
    cnames <- some (some alphaNumOrUnderscore <* optional (optional whitespace <* keyword "," <* optional whitespace))
    _ <- optional whitespace
    _ <- keyword ")"
    _ <- optional whitespace
    _ <- keyword "VALUES"
    _ <- whitespace
    _ <- keyword "("
    values <- some valueParser
    _ <- keyword ")"
    _ <- optional whitespace
    _ <- keyword ";"
    return $ Insert tableName (zip cnames values)

deleteParser :: Parser ParsedStatement
deleteParser = do
    _ <- keyword "DELETE"
    _ <- whitespace
    _ <- keyword "FROM"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    do
        _ <- optional whitespace
        _ <- keyword ";"
        return $ Delete tableName Nothing
        <|> do
            _ <- whitespace
            _ <- keyword "WHERE"
            whereClause <- Just <$> whereParser <|> throwError "Malformed where clause"
            _ <- optional whitespace
            _ <- keyword ";"
            return $ Delete tableName whereClause

dropParser :: Parser ParsedStatement
dropParser = do
    _ <- keyword "DROP"
    _ <- whitespace
    _ <- keyword "TABLE"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- optional whitespace
    _ <- keyword ";"
    return $ Drop tableName

createParser :: Parser ParsedStatement
createParser = do
    _ <- keyword "CREATE"
    _ <- whitespace
    _ <- keyword "TABLE"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- whitespace
    _ <- keyword "("
    _ <- optional whitespace
    colHeader <- some (columnParser <* optional (optional whitespace <* keyword "," <* optional whitespace))
    _ <- optional whitespace
    _ <- keyword ")"
    _ <- optional whitespace
    _ <- keyword ";"
    return $ Create tableName colHeader

columnParser :: Parser Column
columnParser = do
    cname <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- optional whitespace
    ctype <- keyword "INTEGER" *> pure IntegerType <|> keyword "STRING" *> pure StringType <|> keyword "BOOL" *> pure BoolType
    return $ Column cname ctype

valueParser' :: Parser (Cname, Value)
valueParser' = do
    _ <- optional whitespace
    columnName <- some (satisfy isAlphaNum)
    _ <- optional whitespace
    _ <- keyword "="
    _ <- optional whitespace
    value <- valueParser
    _ <- optional whitespace
    return (columnName, value)

updateParser :: Parser ParsedStatement
updateParser = do
    _ <- keyword "UPDATE"
    _ <- whitespace
    tableName <- some (satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c)))
    _ <- whitespace
    _ <- keyword "SET"
    _ <- whitespace
    -- City = 'Frankfurt', Country = 'Germany', Population = 500000
    values <- some valueParser' <* keyword "," <|> some valueParser'
    _ <- optional whitespace
    _ <- keyword "WHERE"
    whereClause <- Just <$> whereParser <|> throwError "Malformed where clause"
    _ <- optional whitespace
    _ <- keyword ";"
    return $ Update tableName values whereClause

nowParser :: Parser ParsedStatement
nowParser = do
    _ <- keyword "SELECT"
    _ <- whitespace
    _ <- keyword "NOW()"
    _ <- optional whitespace
    _ <- keyword ";"
    return ShowTime

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement = 
    case runState (runExceptT (runParser parser)) statement of
        (Left err, _) -> Left err
        (Right parsedStatement, remaining) -> 
            if null remaining 
            then Right parsedStatement 
            else Left $ "Unexpected input: " ++ remaining
  where
    parser = selectParser <|> 
             nowParser <|>
             showTablesParser <|> 
             showTableParser <|> 
             selectAllParser <|>
             insertParser <|>
             updateParser <|>
             deleteParser <|>
             dropParser <|>
             createParser

-- Executes a parsed statemet. Produces a DataFrame.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented"

-- takes dataframe and returns the columns and data types of the table (without the rows)
showTable :: Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
showTable (Left err) = Left err
showTable (Right (DataFrame columns _)) = Right $ DataFrame [Column "Field" StringType, Column "Type" StringType] [[StringValue cname, StringValue (show ctype)] | Column cname ctype <- columns]


-- Where clauses

applyWhereClauses :: Maybe Condition -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyWhereClauses Nothing (Left err) = Left err
applyWhereClauses Nothing (Right df) = Right df
applyWhereClauses _ (Right df@(DataFrame _ [])) = Right df
applyWhereClauses (Just (JoinCondition _ _)) c = c
applyWhereClauses _ (Left err) = Left err
applyWhereClauses (Just condition) (Right (DataFrame columns rows)) = 
    let 
        typesValid = verifyWhereClauseTypes columns condition
    in
        if not typesValid then Left "Cannot apply WHERE clause to non-boolean column"
        else Right (DataFrame columns (filter (applyCondition columns condition) rows))
 
applyCondition :: [Column] -> Condition -> [Value] -> Bool
applyCondition _ (JoinCondition _ _) _ = False
applyCondition columns (And c1 c2) values = applyCondition columns c1 values && applyCondition columns c2 values
applyCondition columns (BoolCondition cname bool) values =
    let columnIdx = getIndex cname columns
        value = values !! columnIdx
    in case value of
        (BoolValue b) -> b == bool
        NullValue -> False
        _ -> error "Non-boolean condition not implemented"
applyCondition columns (Equal cname value) values =
    let columnIdx = getIndex cname columns
        value' = values !! columnIdx
    in case value' of
        (IntegerValue i) -> case value of
            (IntegerValue i') -> i == i'
            NullValue -> False
            _ -> error "Invalid path"
        (BoolValue b) -> case value of
            (BoolValue b') -> b == b'
            NullValue -> False
            _ -> error "Invalid path"
        (StringValue s) -> case value of
            (StringValue s') -> s == s'
            NullValue -> False
            _ -> error "Invalid path"
        NullValue -> False
 
 
verifyWhereClauseTypes :: [Column] -> Condition -> Bool 
verifyWhereClauseTypes _ (JoinCondition _ _) = True
verifyWhereClauseTypes columns (And c1 c2) = verifyWhereClauseTypes columns c1 && verifyWhereClauseTypes columns c2
verifyWhereClauseTypes columns (BoolCondition cname _) =
    case [(ctype, cname) | Column cname' ctype <- columns, cname == cname'] of
        ((BoolType, _):_) -> True
        _ -> False
verifyWhereClauseTypes columns (Equal cname value) =
    let columnType = head [(ctype, cname) | Column cname' ctype <- columns, cname == cname']
    in case columnType of
        (IntegerType, _) -> case value of
            (IntegerValue _) -> True
            NullValue -> True
            _ -> False
        (BoolType, _) -> case value of
            (BoolValue _) -> True
            NullValue -> True
            _ -> False
        (StringType, _) -> case value of
            (StringValue _) -> True
            NullValue -> True
            _ -> False

-- Aggregates
parseColumnExpressions :: [ColumnExpression] -> [(Aggregate, Cname)]
parseColumnExpressions [] = []
parseColumnExpressions (x:xs) = case x of
    SimpleColumn cname -> (None, cname) : parseColumnExpressions xs
    AggregateColumn aggr cname -> (aggr, cname) : parseColumnExpressions xs
    FunctionCall cname -> (None, cname) : parseColumnExpressions xs

applyAggregates :: [ColumnExpression] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyAggregates _ (Left err) = Left err
applyAggregates _ (Right df@(DataFrame _ [])) = Right df
applyAggregates colExp (Right (DataFrame columns rows)) =
    let cols = parseColumnExpressions colExp
        (nones, notNones) = partition (== None) [aggregate | (aggregate, _) <- cols]
        validAggregates = verifyAggregates (nones, notNones)
        validTypes = verifyAggregateTypes columns cols
        columns' = [Column (show aggregate ++ "("++cname++")") ctype | (aggregate, cname) <- cols, Column cname' ctype <- columns, cname == cname']
        valueGroups = transpose rows
        applicator = [if aggregate == Max then applyMaxAggregate else applySumAggregate | (aggregate, _) <- cols]
        rows' = transpose $ zipWith (\f values -> [f values]) applicator valueGroups
    in
        if null cols || null notNones then Right (DataFrame columns rows)
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

verifyAggregateTypes :: [Column] -> [(Aggregate, Cname)] -> Bool
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
-- If a column has an aggregate, check for Aggregate(cname) in the columns of the dataframe
selectColumns :: [ColumnExpression] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
selectColumns _ (Left errorMsg) = Left errorMsg
selectColumns columnExpressions (Right (DataFrame columns rows)) =
    if allColumnsExist columnExpressions columns
    then let transposedRows = transpose rows
             columnNames = mapMaybe columnName columnExpressions
             newColumns = map (findColumn transposedRows) columnNames
             newRows = transpose newColumns
         in Right (DataFrame (map (findColumnHeader columns) columnNames) newRows)
    else Left "One or more columns not found."

  where
    columnName :: ColumnExpression -> Maybe Cname
    columnName (SimpleColumn cname) = Just cname
    columnName (AggregateColumn _ cname) = Just cname
    columnName (FunctionCall cname) = Just cname

    findColumnHeader :: [Column] -> Cname -> Column
    findColumnHeader cols cname = fromMaybe (Column cname StringType) $ find (\(Column name _) -> name == cname) cols

    findColumn :: [[Value]] -> Cname -> [Value]
    findColumn transposedRows cname =
        fromMaybe [] $ lookupColumnValues cname transposedRows

    lookupColumnValues :: Cname -> [[Value]] -> Maybe [Value]
    lookupColumnValues cname transposedRows =
        findIndex (\(Column name _) -> name == cname) columns >>= \idx -> Just (transposedRows !! idx)

    allColumnsExist :: [ColumnExpression] -> [Column] -> Bool
    allColumnsExist exprs cols = all ( \case
                                        SimpleColumn cname -> columnExists cname cols
                                        AggregateColumn _ cname -> columnExists cname cols
                                        FunctionCall _ -> True
                                      ) exprs

    columnExists :: Cname -> [Column] -> Bool
    columnExists cname = any (\(Column name _) -> name == cname)

-- Apply time function
applyTimeFunction :: [ColumnExpression] -> UTCTime -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyTimeFunction _ _ (Left errorMsg) = Left errorMsg
applyTimeFunction _ time (Right (DataFrame columns rows)) =
    let timeColIndex = fromMaybe (-1) (findIndex (\(Column name _) -> name == "NOW()") columns)
    in 
        if timeColIndex == -1 then Right (DataFrame columns rows) 
        else Right (DataFrame columns (map (insertAt timeColIndex (TimeValue (show time))) rows))
    where
        insertAt :: Int -> a -> [a] -> [a]
        insertAt idx el xs = let (ys, zs) = splitAt idx xs in ys ++ [el] ++ zs

applyOrder :: Maybe [OrderBy] -> Either ErrorMessage DataFrame -> Either ErrorMessage DataFrame
applyOrder Nothing df = df
applyOrder _ (Left errorMsg) = Left errorMsg
applyOrder (Just orderBys) (Right (DataFrame columns rows)) =
    Right . DataFrame columns $ sortBy compositeCompareFunc rows
    where
        compositeCompareFunc :: [Value] -> [Value] -> Ordering
        compositeCompareFunc row1 row2 = foldl (\acc orderBy -> if acc == EQ then compareBy orderBy row1 row2 else acc) EQ orderBys

        compareBy :: OrderBy -> [Value] -> [Value] -> Ordering
        compareBy (OrderBy cname ord) row1 row2 =
            let colIdx = fromMaybe (error $ "Column not found: " ++ cname) 
                                   (findIndex (\(Column name _) -> name == cname) columns)
                compareFunc = case ord of
                    Asc -> compareValue
                    Desc -> flip compareValue
            in compareFunc (row1 !! colIdx) (row2 !! colIdx)

-- Helper functions
compareValue :: Value -> Value -> Ordering
compareValue (IntegerValue a) (IntegerValue b) = compare a b
compareValue (StringValue a) (StringValue b) = compare a b
compareValue (BoolValue a) (BoolValue b) = compare a b
compareValue (TimeValue a) (TimeValue b) = compare a b
compareValue NullValue NullValue = EQ
compareValue _ _ = error "Comparison between different Value types not supported (Invalid path))"

alphaNumOrUnderscore :: Parser Char
alphaNumOrUnderscore = satisfy (\c -> isAlphaNum c || c == '_' && not (isSpace c))

getIndex :: String -> [Column] -> Int
getIndex name cols = case  name  `elemIndex` [cname | Column cname _ <- cols] of
  Just idx -> idx
  Nothing  -> error ("Column " ++ name ++ " not found")

safeMaximum :: Ord a => [a] -> Maybe a
safeMaximum [] = Nothing
safeMaximum xs = Just $ maximum xs
