import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import TestInterpreter
import DataFrame
import Test.Hspec
import Util
import Data.IORef (newIORef)

main :: IO ()
main = hspec $ do
  describe "Unit tests" $ do
    describe "Lib1.findTableByName" $ do
      it "handles empty lists" $ do
        Lib1.findTableByName [] "" `shouldSatisfy` 
          isLeft
  
      it "handles empty names" $ do
        Lib1.findTableByName D.database "" `shouldSatisfy` 
          isLeft
  
      it "can find by name" $ do
        Lib1.findTableByName D.database "employees" `shouldBe` 
          Right (snd D.tableEmployees)
  
      it "can't find by case-insensitive name" $ do
        Lib1.findTableByName D.database "employEEs" `shouldSatisfy` 
          isLeft
  
    describe "Lib1.parseSelectAllStatement" $ do
      it "handles empty input" $ do
        Lib1.parseSelectAllStatement "" `shouldSatisfy` 
          isLeft
  
      it "handles invalid queries" $ do
        Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` 
          isLeft
  
      it "returns table name from correct queries" $ do
        Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` 
          Right "dual"
  
    describe "Lib1.validateDataFrame" $ do
      it "finds types mismatch" $ do
        Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` 
          isLeft
  
      it "finds column size mismatch" $ do
        Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` 
          isLeft
  
      it "reports different error messages" $ do
        Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` 
          Lib1.validateDataFrame (snd D.tableInvalid2)
  
      it "passes valid tables" $ do
        Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` 
          Right ()
  
    describe "Lib1.renderDataFrameAsTable" $ do
      it "renders a table" $ do
        Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` 
          not . null
  
    describe "Lib2.parseStatement" $ do
      it "handles basic SELECT statement" $ do
        parseStatement "SELECT id FROM employees;" `shouldBe`
          Right (Select [SimpleColumn "id"] ["employees"] Nothing)
  
      it "handles multiple column SELECT" $ do
        parseStatement "SELECT name, surname FROM employees;" `shouldBe` 
          Right (Select [SimpleColumn "name", SimpleColumn "surname"] ["employees"] Nothing)
  
      it "parses SUM aggregate function" $ do
        parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` 
          Right (Select [AggregateColumn Sum "id"] ["employees"] Nothing)
  
      it "parses MAX aggregate function" $ do
        parseStatement "SELECT MAX(name) FROM employees;" `shouldBe` 
          Right (Select [AggregateColumn Max "name"] ["employees"] Nothing)
  
      it "parses WHERE clause with AND" $ do
        parseStatement "SELECT id FROM employees WHERE name IS TRUE AND surname IS FALSE;" `shouldBe` 
          Right (Select [SimpleColumn "id"] ["employees"] (Just (And (BoolCondition "name" True) (BoolCondition "surname" False))))
  
      it "parses WHERE clause with single condition" $ do
        parseStatement "SELECT id FROM employees WHERE name IS TRUE;" `shouldBe` 
          Right (Select [SimpleColumn "id"] ["employees"] (Just (BoolCondition "name" True)))
  
      it "handles case-insensitive SELECT keyword" $ do
        parseStatement "sEleCt name FROM employees;" `shouldBe` Right 
          (Select [SimpleColumn "name"] ["employees"] Nothing)
          
      it "handles case-insensitive FROM keyword" $ do
        parseStatement "SELECT name frOm employees;" `shouldBe` 
          Right (Select [SimpleColumn "name"] ["employees"] Nothing)
  
      it "handles case-insensitive WHERE keyword" $ do
        parseStatement "SELECT name FROM employees wHeRe name IS TRUE;" `shouldBe` 
          Right (Select [SimpleColumn "name"] ["employees"] (Just (BoolCondition "name" True)))
  
      it "handles case-sensitive table names" $ do
        parseStatement "SELECT name FROM Employees;" `shouldBe` 
          Right (Select [SimpleColumn "name"] ["Employees"] Nothing)
  
      it "handles case-sensitive column names" $ do
        parseStatement "SELECT NAME FROM employees;" `shouldBe` 
          Right (Select [SimpleColumn "NAME"] ["employees"] Nothing)
  
      it "parses SHOW TABLES statement" $ do
        parseStatement "SHOW TABLES;" `shouldBe` 
          Right ShowTables
  
      it "parses SHOW TABLE with table name" $ do
        parseStatement "SHOW TABLE employees;" `shouldBe` 
          Right (ShowTable "employees")
  
      it "returns error for invalid statement" $ do
        parseStatement "INVALID STATEMENT" `shouldSatisfy` 
          isLeft
  
    --describe "Lib2.showTables" $ do
      --it "returns table names" $ do
        --showTables D.database `shouldBe`
          --Right (DataFrame [Column "Tables_in_database" StringType] [[StringValue "employees"],[StringValue "invalid1"],[StringValue "invalid2"],[StringValue "long_strings"],[StringValue "flags"]])
  
    describe "Lib2.showTable" $ do
      it "returns table" $ do
        showTable (Right $ snd D.tableWithNulls) `shouldBe`
          Right (DataFrame [Column "Field" StringType,Column "Type" StringType] [[StringValue "flag",StringValue "StringType"],[StringValue "value",StringValue "BoolType"]])
  
      it "handles empty dataframe" $ do
        showTable (Right $ DataFrame [] []) `shouldBe`
          Right (DataFrame [Column "Field" StringType,Column "Type" StringType] [])
  
    describe "Lib2.applyWhereClauses" $ do
      it "handles empty criteria" $ do
        applyWhereClauses Nothing (Right $ snd D.tableEmployees) `shouldBe` 
          Right (snd D.tableEmployees)
  
      it "handles empty DataFrame" $ do
        applyWhereClauses (Just $ BoolCondition "name" True) (Right $ DataFrame [] []) `shouldBe`
          Right (DataFrame [] [])
      
      it "handles invalid DataFrame" $ do
        applyWhereClauses (Just $ BoolCondition "name" True) (Right $ snd D.tableInvalid1) `shouldSatisfy` 
          isLeft
  
      it "handles invalid criteria" $ do
        applyWhereClauses (Just $ BoolCondition "name" True) (Right $ snd D.tableEmployees) `shouldSatisfy` 
          isLeft
     
      it "handles non-existent column" $ do
        applyWhereClauses (Just $ BoolCondition "NAME" True) (Right $ snd D.tableEmployees) `shouldSatisfy`
          isLeft
  
      it "filters true values" $ do
        applyWhereClauses (Just $ BoolCondition "value" True) (Right $ snd D.tableWithNulls) `shouldBe`
          Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "b", BoolValue True]])
  
      it "filters false values" $ do
        applyWhereClauses (Just $ BoolCondition "value" False) (Right $ snd D.tableWithNulls) `shouldBe`
          Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "b", BoolValue False]])
  
      it "handles multiple conditions" $ do
        let df = DataFrame [Column "flag" StringType, Column "value1" BoolType, Column "value2" BoolType] [[StringValue "YES", BoolValue True, BoolValue False], [StringValue "NO", BoolValue True, BoolValue True], [StringValue "NO", BoolValue False, BoolValue False]]
        let cond = And (BoolCondition "value1" True) (BoolCondition "value2" False)
        applyWhereClauses (Just cond) (Right df) `shouldBe`
          Right (DataFrame [Column "flag" StringType, Column "value1" BoolType, Column "value2" BoolType] [[StringValue "YES", BoolValue True, BoolValue False]])
  
    describe "Lib2.applyAggregates" $ do
      it "handles invalid criteria" $ do
        let selectedColumns = selectColumns [SimpleColumn "name"] (Right $ snd D.tableEmployees)
        applyAggregates [SimpleColumn "name"] selectedColumns `shouldBe` 
         selectedColumns
  
      it "handles SUM aggregate" $ do
        let selectedColumns = selectColumns [AggregateColumn Sum "id"] (Right $ snd D.tableEmployees)
        applyAggregates [AggregateColumn Sum "id"] selectedColumns `shouldBe` 
          Right (DataFrame [Column "Sum(id)" IntegerType] [[IntegerValue 3]])
   
      it "handles MAX aggregate (int)" $ do
        let selectedColumns = selectColumns [AggregateColumn Max "id"] (Right $ snd D.tableEmployees)
        applyAggregates [AggregateColumn Max "id"] selectedColumns `shouldBe` 
          Right (DataFrame [Column "Max(id)" IntegerType] [[IntegerValue 2]])
      
      it "handles MAX aggregate (string)" $ do
        let selectedColumns = selectColumns [AggregateColumn Max "name"] (Right $ snd D.tableEmployees)
        applyAggregates [AggregateColumn Max "name"] selectedColumns `shouldBe` 
          Right (DataFrame [Column "Max(name)" StringType] [[StringValue "Vi"]])
  
      it "handles MAX aggregate (bool)" $ do
        let selectedColumns = selectColumns [AggregateColumn Max "value"] (Right $ snd D.tableWithNulls)
        applyAggregates [AggregateColumn Max "value"] selectedColumns `shouldBe` 
          Right (DataFrame [Column "Max(value)" BoolType] [[BoolValue True]])
  
    describe "Lib2.selectColumns" $ do
      it "handles empty criteria and Left DataFrame" $ do
        selectColumns [] (Left "") `shouldSatisfy` 
          isLeft
  
      it "filters columns based on criteria" $ do
        selectColumns [SimpleColumn "name"] (Right $ snd D.tableEmployees) `shouldBe` 
          Right (DataFrame [Column "name" StringType] [[StringValue "Vi"],[StringValue "Ed"]])
  
      it "filters columns based on other criteria" $ do
        selectColumns [AggregateColumn Sum "id"] (Right $ snd D.tableEmployees) `shouldBe` 
          Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2]])
      
      it "filters columns based on other other criteria" $ do
        selectColumns [SimpleColumn "surname"] (Right $ snd D.tableEmployees) `shouldBe` 
          Right (DataFrame [Column "surname" StringType] [[StringValue "Po"], [StringValue "Dl"]])
  
      it "handles non-existent columns" $ do
        selectColumns [AggregateColumn Max "IMNOTREAL"] (Right $ snd D.tableEmployees) `shouldSatisfy` 
          isLeft
  
      it "handles invalid DataFrame" $ do
        selectColumns [SimpleColumn "name"] (Right $ DataFrame [] []) `shouldSatisfy` 
          isLeft
  
      it "handles column order correctly" $ do
        selectColumns [SimpleColumn "surname", SimpleColumn "name"] (Right $ snd D.tableEmployees) `shouldBe` 
          Right (DataFrame [Column "surname" StringType, Column "name" StringType] [[StringValue "Po", StringValue "Vi"], [StringValue "Dl", StringValue "Ed"]])
  
    describe "Lib3.serializeDataFrame" $ do
      it "serializes a DataFrame with valid data" $ do
        let df = DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"], [IntegerValue 2, StringValue "Bob"]]
        serializeDataFrame (Right df) `shouldBe`
          Right "columns:\n  - name: id\n    type: IntegerType\n  - name: name\n    type: StringType\n\nrows:\n  - id: 1\n    name: Alice\n  - id: 2\n    name: Bob\n" 
  
      it "serializes an empty DataFrame" $ do
        let df = DataFrame [] []
        serializeDataFrame (Right df) `shouldBe`
          Right"columns:\n\nrows:\n" 
  
      it "returns error for Left input" $ do
        serializeDataFrame (Left "Error occurred") `shouldBe`
          Left "Error occurred"
  
    describe "Lib3.deserializeDataFrame" $ do
      it "deserializes valid serialized data" $ do
        let serialized = "columns:\n  - name: id\n    type: IntegerType\n  - name: name\n    type: StringType\nrows:\n  - id: 1\n    name: Alice\n  - id: 2\n    name: Bob\n"
        deserializeDataFrame (Right serialized) `shouldBe`
          Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"], [IntegerValue 2, StringValue "Bob"]])
  
      it "returns error on invalid serialized data" $ do
        deserializeDataFrame (Right "invalid data") `shouldSatisfy` isLeft
  
      it "returns error for Left input" $ do
        deserializeDataFrame (Left "Error occurred") `shouldBe`
          Left "Error occurred"
    
    describe "Lib3.dataFrameComplement" $ do
      it "returns rows from df1 not in df2" $ do
        let df1 = DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2], [IntegerValue 3]]
        let df2 = DataFrame [Column "id" IntegerType] [[IntegerValue 3], [IntegerValue 4]]
        dataFrameComplement (Right df1) (Right df2) `shouldBe`
          Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2]])
    
      it "handles empty rows in df1" $ do
        let df1 = DataFrame [Column "id" IntegerType] []
        let df2 = DataFrame [Column "id" IntegerType] [[IntegerValue 1]]
        dataFrameComplement (Right df1) (Right df2) `shouldBe`
          Right (DataFrame [Column "id" IntegerType] [])
    
      it "handles empty rows in df2" $ do
        let df1 = DataFrame [Column "id" IntegerType] [[IntegerValue 1]]
        let df2 = DataFrame [Column "id" IntegerType] []
        dataFrameComplement (Right df1) (Right df2) `shouldBe`
          Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
    
      it "handles different columns in df1 and df2" $ do
        let df1 = DataFrame [Column "id" IntegerType] [[IntegerValue 1]]
        let df2 = DataFrame [Column "name" StringType] [[StringValue "Alice"]]
        dataFrameComplement (Right df1) (Right df2) `shouldBe`
          Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
    
      it "returns error for Left input in df1" $ do
        let df2 = DataFrame [Column "id" IntegerType] [[IntegerValue 1]]
        dataFrameComplement (Left "Error in df1") (Right df2) `shouldBe`
          Left "Error in df1"
    
      it "returns error for Left input in df2" $ do
        let df1 = DataFrame [Column "id" IntegerType] [[IntegerValue 1]]
        dataFrameComplement (Right df1) (Left "Error in df2") `shouldBe`
          Left "Error in df2"
    
      it "returns error for Left inputs in both df1 and df2" $ do
        dataFrameComplement (Left "Error in df1") (Left "Error in df2") `shouldSatisfy`
          isLeft
  
    describe "Util.matchCondition" $ do
      it "matches Equal condition correctly" $ do
        let condition = Lib2.Equal "id" (IntegerValue 2)
        let columns = [Column "id" IntegerType, Column "name" StringType]
        let row = [IntegerValue 2, StringValue "Alice"]
        matchCondition condition columns row `shouldBe` True
    
      it "does not match incorrect Equal condition" $ do
        let condition = Lib2.Equal "id" (IntegerValue 3)
        let columns = [Column "id" IntegerType, Column "name" StringType]
        let row = [IntegerValue 2, StringValue "Alice"]
        matchCondition condition columns row `shouldBe` False
    
      it "matches And condition correctly" $ do
        let condition = Lib2.And (Lib2.Equal "id" (IntegerValue 2)) (Lib2.BoolCondition "active" True)
        let columns = [Column "id" IntegerType, Column "name" StringType, Column "active" BoolType]
        let row = [IntegerValue 2, StringValue "Alice", BoolValue True]
        matchCondition condition columns row `shouldBe` True
    
      it "does not match incorrect And condition" $ do
        let condition = Lib2.And (Lib2.Equal "id" (IntegerValue 2)) (Lib2.BoolCondition "active" False)
        let columns = [Column "id" IntegerType, Column "name" StringType, Column "active" BoolType]
        let row = [IntegerValue 2, StringValue "Alice", BoolValue True]
        matchCondition condition columns row `shouldBe` False
    
      it "matches BoolCondition correctly" $ do
        let condition = Lib2.BoolCondition "active" True
        let columns = [Column "id" IntegerType, Column "name" StringType, Column "active" BoolType]
        let row = [IntegerValue 2, StringValue "Alice", BoolValue True]
        matchCondition condition columns row `shouldBe` True
    
      it "does not match incorrect BoolCondition" $ do
        let condition = Lib2.BoolCondition "active" False
        let columns = [Column "id" IntegerType, Column "name" StringType, Column "active" BoolType]
        let row = [IntegerValue 2, StringValue "Alice", BoolValue True]
        matchCondition condition columns row `shouldBe` False
      
    describe "Util.joinTables" $ do
      it "joins two tables correctly without condition" $ do
        let df1 = Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
        let df2 = Right (DataFrame [Column "name" StringType] [[StringValue "Alice"]])
        joinTables Nothing df1 df2 `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"]])
    
      it "joins two tables with JoinCondition correctly" $ do
        let df1 = Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"]])
        let df2 = Right (DataFrame [Column "id" IntegerType, Column "city" StringType] [[IntegerValue 1, StringValue "Paris"]])
        joinTables (Just (Lib2.JoinCondition "id" "id")) df1 df2 `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "id" IntegerType, Column "city" StringType] [[IntegerValue 1, StringValue "Alice", IntegerValue 1, StringValue "Paris"]])
    
      it "returns error if columns for JoinCondition are not found" $ do
        let df1 = Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
        let df2 = Right (DataFrame [Column "name" StringType] [[StringValue "Alice"]])
        joinTables (Just (Lib2.JoinCondition "nonexistent" "id")) df1 df2 `shouldSatisfy` isLeft
  
    describe "Util.insertInto" $ do
      it "inserts row into DataFrame correctly" $ do
        let df = Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"]])
        insertInto [("id", IntegerValue 2), ("name", StringValue "Bob")] df `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"], [IntegerValue 2, StringValue "Bob"]])
    
      it "returns error if column not found" $ do
        let df = Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
        insertInto [("nonexistent", IntegerValue 2)] df `shouldSatisfy` isLeft
    
      it "returns error if type mismatch" $ do
        let df = Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
        insertInto [("id", StringValue "2")] df `shouldSatisfy` isLeft
  
    describe "Util.updateTableDataFrame" $ do
      it "updates rows in DataFrame correctly" $ do
        let df = Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"], [IntegerValue 2, StringValue "Bob"]])
        updateTableDataFrame [("name", StringValue "Updated")] (Just (Lib2.Equal "id" (IntegerValue 1))) df `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Updated"], [IntegerValue 2, StringValue "Bob"]])
    
      it "updates all rows if condition is Nothing" $ do
        let df = Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Alice"], [IntegerValue 2, StringValue "Bob"]])
        updateTableDataFrame [("name", StringValue "Updated")] Nothing df `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Updated"], [IntegerValue 2, StringValue "Updated"]])
    
      it "returns error if column for update not found" $ do
        let df = Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1]])
        updateTableDataFrame [("nonexistent", StringValue "Updated")] Nothing df `shouldSatisfy` isLeft
  describe "Integration tests" $ do
    let mockDB = D.database
    it "handles SELECT * FROM employees;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT * FROM employees2;")
      result `shouldBe` (Right . snd $ D.employees2)
    it "handles SELECT name FROM employees WHERE active IS TRUE;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT * FROM employees2 WHERE active IS TRUE;")
      result `shouldBe` Right (DataFrame [ Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType ] [ [IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 70000, BoolValue True], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True]]) 
    it "handles SELECT name, active, id FROM employees WHERE active IS TRUE;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT name, active, id FROM employees2 WHERE active IS TRUE;")
      result `shouldBe` Right (DataFrame [Column "name" StringType, Column "active" BoolType, Column "id" IntegerType] [[StringValue "Alice", BoolValue True, IntegerValue 1], [StringValue "Charlie", BoolValue True, IntegerValue 3]]) 
    it "handles SELECT SUM(salary) FROM employees WHERE department = 'Engineering';" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT SUM(salary) FROM employees2 WHERE department = 'Engineering';")
      result `shouldBe` Right (DataFrame [Column "Sum(salary)" IntegerType] [[IntegerValue 70000]])
    it "handles SELECT MAX(salary) FROM employees;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT MAX(salary) FROM employees2;")
      result `shouldBe` Right (DataFrame [Column "Max(salary)" IntegerType] [[IntegerValue 70000]])
    it "handles SHOW TABLE employees2" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SHOW TABLE employees2;")
      result `shouldBe` Right (DataFrame [Column "Field" StringType,Column "Type" StringType] [[StringValue "id",StringValue "IntegerType"],[StringValue "name",StringValue "StringType"],[StringValue "department",StringValue "StringType"],[StringValue "salary",StringValue "IntegerType"],[StringValue "active",StringValue "BoolType"]])
    it "handles INSERT INTO employees (id, name, department, salary, active) VALUES (4, 'Diana', 'HR', 68000, TRUE);" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "INSERT INTO employees2 (id, name, department, salary, active) VALUES (4, 'Diana', 'HR', 68000, TRUE);")
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType] [[IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 70000, BoolValue True], [IntegerValue 2, StringValue "Bob", StringValue "Marketing", IntegerValue 60000, BoolValue False], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True], [IntegerValue 4, StringValue "Diana", StringValue "HR", IntegerValue 68000, BoolValue True]])
    it "handles UPDATE employees SET salary = 72000 WHERE id = 1;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "UPDATE employees2 SET salary = 72000 WHERE id = 1;")
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType] [[IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 72000, BoolValue True], [IntegerValue 2, StringValue "Bob", StringValue "Marketing", IntegerValue 60000, BoolValue False], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True]])
    it "handles DELETE FROM employees WHERE active IS FALSE;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "DELETE FROM employees2 WHERE active IS FALSE;")
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType] [[IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 70000, BoolValue True], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True]])
    it "handles DELETE FROM employees WHERE active IS FALSE;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "DELETE FROM employees2 WHERE active IS FALSE;")
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType] [[IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 70000, BoolValue True], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True]])
    it "handles SELECT NOW();" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT NOW();")
      result `shouldSatisfy` isRight
    it "handles SELECT name, department, manager FROM employees, departments WHERE department = dept_name;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT name, department, manager FROM employees2, departments WHERE department = dept_name;")
      result `shouldBe` Right (DataFrame [Column "name" StringType, Column "department" StringType, Column "manager" StringType] [[StringValue "Alice", StringValue "Engineering", StringValue "Alice"], [StringValue "Bob", StringValue "Marketing", StringValue "Evan"], [StringValue "Charlie", StringValue "Sales", StringValue "Charlie"]])
    it "handles DELETE FROM employees;" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "DELETE FROM employees2;")
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType] [])
    it "handles missing FROM" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT id, name;")
      result `shouldSatisfy` isLeft
    it "handles incorrect aggregate function" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT SUM(name) FROM employees2;")
      result `shouldSatisfy` isLeft
    it "handles mismatched quotes" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT name FROM employees2 WHERE name = 'Alice;")
      result `shouldSatisfy` isLeft
    it "handles missing closing parenthesis" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT MAX(name FROM employees2;")
      result `shouldSatisfy` isLeft
    it "handles non-existent column" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT nonexistent FROM employees2;")
      result `shouldSatisfy` isLeft
    it "handles non-existent table" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT * FROM nonexistent;")
      result `shouldSatisfy` isLeft
    it "handles mixing of aggregate and non-aggregate columns" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT name, MAX(salary) FROM employees2;")
      result `shouldSatisfy` isLeft
    it "handles invalid data type in WHERE clause" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT * FROM employees2 WHERE name = 1;")
      result `shouldSatisfy` isLeft
    it "handles invalid data type in INSERT INTO" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "INSERT INTO employees2 (id, name, department, salary, active) VALUES ('ABC', 2, 'HR', '68000', TRUE);")
      result `shouldSatisfy` isLeft
    it "handles using aggregate function in WHERE clause" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "SELECT * FROM employees2 WHERE MAX(salary) = 70000;")
      result `shouldSatisfy` isLeft
    it "handles invalid data type in UPDATE" $ do
      mockDbRef <- newIORef mockDB
      result <- runTestInterpreter mockDbRef (executeSql "UPDATE employees2 SET salary = '72000' WHERE id = 1;")
      result `shouldSatisfy` isLeft
