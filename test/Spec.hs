import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import DataFrame
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` 
        Nothing

    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` 
        Nothing

    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` 
        Just (snd D.tableEmployees)

    --it "can find by case-insensitive name" $ do
      --Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)

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
        Right (Select [(None, "id")] "employees" Nothing)

    it "handles multiple column SELECT" $ do
      parseStatement "SELECT name, surname FROM employees;" `shouldBe` 
        Right (Select [(None, "name"), (None, "surname")] "employees" Nothing)

    it "parses SUM aggregate function" $ do
      parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` 
        Right (Select [(Sum, "id")] "employees" Nothing)

    it "parses MAX aggregate function" $ do
      parseStatement "SELECT MAX(name) FROM employees;" `shouldBe` 
        Right (Select [(Max, "name")] "employees" Nothing)

    it "parses WHERE clause with AND" $ do
      parseStatement "SELECT id FROM employees WHERE name IS TRUE AND surname IS FALSE;" `shouldBe` 
        Right (Select [(None, "id")] "employees" (Just (And (BoolCondition "name" True) (BoolCondition "surname" False))))

    it "parses WHERE clause with single condition" $ do
      parseStatement "SELECT id FROM employees WHERE name IS TRUE;" `shouldBe` 
        Right (Select [(None, "id")] "employees" (Just (BoolCondition "name" True)))

    it "handles case-insensitive SELECT keyword" $ do
      parseStatement "sEleCt name FROM employees;" `shouldBe` Right 
        (Select [(None, "name")] "employees" Nothing)
        
    it "handles case-insensitive FROM keyword" $ do
      parseStatement "SELECT name frOm employees;" `shouldBe` 
        Right (Select [(None, "name")] "employees" Nothing)

    it "handles case-insensitive WHERE keyword" $ do
      parseStatement "SELECT name FROM employees wHeRe name IS TRUE;" `shouldBe` 
        Right (Select [(None, "name")] "employees" (Just (BoolCondition "name" True)))

    it "handles case-sensitive table names" $ do
      parseStatement "SELECT name FROM Employees;" `shouldBe` 
        Right (Select [(None, "name")] "Employees" Nothing)

    it "handles case-sensitive column names" $ do
      parseStatement "SELECT NAME FROM employees;" `shouldBe` 
        Right (Select [(None, "NAME")] "employees" Nothing)

    it "parses SHOW TABLES statement" $ do
      parseStatement "SHOW TABLES;" `shouldBe` 
        Right ShowTables

    it "parses SHOW TABLE with table name" $ do
      parseStatement "SHOW TABLE employees;" `shouldBe` 
        Right (ShowTable "employees")

    it "returns error for invalid statement" $ do
      parseStatement "INVALID STATEMENT" `shouldSatisfy` 
        isLeft

  describe "Lib2.showTables" $ do
    it "returns table names" $ do
      showTables D.database `shouldBe`
        Right (DataFrame [Column "Tables_in_database" StringType] [[StringValue "employees"],[StringValue "invalid1"],[StringValue "invalid2"],[StringValue "long_strings"],[StringValue "flags"]])

  describe "Lib2.showTable" $ do
    it "returns table" $ do
      showTable D.database "flags" `shouldBe`
        Right (DataFrame [Column "Field" StringType,Column "Type" StringType] [[StringValue "flag",StringValue "StringType"],[StringValue "value",StringValue "BoolType"]])

    it "handles invalid name" $ do
      showTable D.database "hello there" `shouldSatisfy`
        isLeft

  describe "Lib2.applyWhereClauses" $ do
    it "handles empty criteria" $ do
      applyWhereClauses Nothing (Just $ snd D.tableEmployees) `shouldBe` 
        Right (snd D.tableEmployees)

    it "handles empty DataFrame" $ do
      applyWhereClauses (Just $ BoolCondition "name" True) (Just $ DataFrame [] []) `shouldSatisfy` 
        isLeft
    
    it "handles invalid DataFrame" $ do
      applyWhereClauses (Just $ BoolCondition "name" True) (Just $ snd D.tableInvalid1) `shouldSatisfy` 
        isLeft

    it "handles invalid criteria" $ do
      applyWhereClauses (Just $ BoolCondition "name" True) (Just $ snd D.tableEmployees) `shouldSatisfy` 
        isLeft
   
    it "handles non-existent column" $ do
      applyWhereClauses (Just $ BoolCondition "NAME" True) (Just $ snd D.tableEmployees) `shouldSatisfy`
        isLeft

    it "filters true values" $ do
      applyWhereClauses (Just $ BoolCondition "value" True) (Just $ snd D.tableWithNulls) `shouldBe`
        Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "a", BoolValue True], [StringValue "b", BoolValue True]])

    it "filters false values" $ do
      applyWhereClauses (Just $ BoolCondition "value" False) (Just $ snd D.tableWithNulls) `shouldBe`
        Right (DataFrame [Column "flag" StringType, Column "value" BoolType] [[StringValue "b", BoolValue False]])

    it "handles multiple conditions" $ do
      let df = DataFrame [Column "flag" StringType, Column "value1" BoolType, Column "value2" BoolType] [[StringValue "YES", BoolValue True, BoolValue False], [StringValue "NO", BoolValue True, BoolValue True], [StringValue "NO", BoolValue False, BoolValue False]]
      let cond = And (BoolCondition "value1" True) (BoolCondition "value2" False)
      applyWhereClauses (Just cond) (Just df) `shouldBe`
        Right (DataFrame [Column "flag" StringType, Column "value1" BoolType, Column "value2" BoolType] [[StringValue "YES", BoolValue True, BoolValue False]])

  describe "Lib2.applyAggregates" $ do
    it "handles invalid criteria" $ do
      let selectedColumns = selectColumns [(Sum, "name")] (Right $ snd D.tableEmployees)
      applyAggregates [(Sum, "name")] selectedColumns `shouldSatisfy` 
        isLeft

    it "handles SUM aggregate" $ do
      let selectedColumns = selectColumns [(Sum, "id")] (Right $ snd D.tableEmployees)
      applyAggregates [(Sum, "id")] selectedColumns `shouldBe` 
        Right (DataFrame [Column "Sum(id)" IntegerType] [[IntegerValue 3]])
 
    it "handles MAX aggregate (int)" $ do
      let selectedColumns = selectColumns [(Max, "id")] (Right $ snd D.tableEmployees)
      applyAggregates [(Max, "id")] selectedColumns `shouldBe` 
        Right (DataFrame [Column "Max(id)" IntegerType] [[IntegerValue 2]])
    
    it "handles MAX aggregate (string)" $ do
      let selectedColumns = selectColumns [(Max, "name")] (Right $ snd D.tableEmployees)
      applyAggregates [(Max, "name")] selectedColumns `shouldBe` 
        Right (DataFrame [Column "Max(name)" StringType] [[StringValue "Vi"]])

    it "handles MAX aggregate (bool)" $ do
      let selectedColumns = selectColumns [(Max, "value")] (Right $ snd D.tableWithNulls)
      applyAggregates [(Max, "value")] selectedColumns `shouldBe` 
        Right (DataFrame [Column "Max(value)" BoolType] [[BoolValue True]])

  describe "Lib2.selectColumns" $ do
    it "handles empty criteria and Left DataFrame" $ do
      selectColumns [] (Left "") `shouldSatisfy` 
        isLeft

    it "filters columns based on criteria" $ do
      selectColumns [(Sum, "name")] (Right $ snd D.tableEmployees) `shouldBe` 
        Right (DataFrame [Column "name" StringType] [[StringValue "Vi"],[StringValue "Ed"]])

    it "filters columns based on other criteria" $ do
      selectColumns [(Sum, "id")] (Right $ snd D.tableEmployees) `shouldBe` 
        Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2]])
    
    it "filters columns based on other other criteria" $ do
      selectColumns [(Sum, "surname")] (Right $ snd D.tableEmployees) `shouldBe` 
        Right (DataFrame [Column "surname" StringType] [[StringValue "Po"], [StringValue "Dl"]])

    it "handles non-existent columns" $ do
      selectColumns [(Max, "NAME")] (Right $ snd D.tableEmployees) `shouldSatisfy` 
        isLeft

    it "handles invalid DataFrame" $ do
      selectColumns [(None, "Name")] (Right $ DataFrame [] []) `shouldSatisfy` 
        isLeft

    it "handles column order correctly" $ do
      selectColumns [(Sum, "surname"), (None, "name")] (Right $ snd D.tableEmployees) `shouldBe` 
        Right (DataFrame [Column "surname" StringType, Column "name" StringType] [[StringValue "Po", StringValue "Vi"], [StringValue "Dl", StringValue "Ed"]])
