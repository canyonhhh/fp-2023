module TestInterpreter where

import Data.IORef
import Control.Monad.Free
import Lib1
import Lib2
import Lib3
import Util
import DataFrame
import InMemoryTables

type MockDatabase = [(TableName, DataFrame)]

runTestInterpreter :: MockDatabase -> Lib3.Execution a -> IO a
runTestInterpreter mockDB (Pure r) = return r
runTestInterpreter mockDB (Free command) = 
    case command of
        Lib3.LoadFile tableName next -> do
            let fileContent = findTableByName mockDB tableName
            runTestInterpreter mockDB (next fileContent)
        Lib3.ReportError err next ->
            putStrLn ("Error in Test Interpreter: " ++ err) >> runTestInterpreter mockDB (next ())
        Lib3.ParseDataFrame fileContent next ->
            runTestInterpreter mockDB (next (parseDataFrame fileContent))
        Lib3.GetTime next ->
        Lib3.SaveTableData tableName dataframe next ->
        Lib3.FilterRows maybeCondition dataframe next ->
        Lib2.applyWhereClauses maybeCondition dataframe ->
        Lib3.JoinTables maybeCondition df1 df2 next ->
        Lib3.InsertInto values dataframe next ->
        Lib3.AggregateData aggregates dataframe next ->
        Lib3.SelectColumns aggregates dataframe next ->
        Lib3.UpdateTableDataFrame dataframe updates maybeCondition next ->
        Lib3.ReportError err next ->
        Lib3.GetTableNames next ->


