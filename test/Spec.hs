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
      Lib1.findTableByName [] "" `shouldBe` Nothing

    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing

    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)

    --it "can find by case-insensitive name" $ do
      --Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)

  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft

    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft

    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"

  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft

    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft

    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)

    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()

  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

  describe "Lib2.parseStatement" $ do
    it "handles basic SELECT statement" $ do
      parseStatement "SELECT id FROM employees;" `shouldBe` Right (Select [(None, "id")] "employees" Nothing)

    it "handles multiple column SELECT" $ do
      parseStatement "SELECT name, surname FROM employees;" `shouldBe` Right (Select [(None, "name"), (None, "surname")] "employees" Nothing)

    it "parses SUM aggregate function" $ do
      parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` Right (Select [(Sum, "id")] "employees" Nothing)

    it "parses MAX aggregate function" $ do
      parseStatement "SELECT MAX(name) FROM employees;" `shouldBe` Right (Select [(Max, "name")] "employees" Nothing)

    it "parses WHERE clause with AND" $ do
      parseStatement "SELECT id FROM employees WHERE name IS TRUE AND surname IS FALSE;" `shouldBe` Right (Select [(None, "id")] "employees" (Just (And (BoolCondition "name" True) (BoolCondition "surname" False))))

    it "parses WHERE clause with single condition" $ do
      parseStatement "SELECT id FROM employees WHERE name IS TRUE;" `shouldBe` Right (Select [(None, "id")] "employees" (Just (BoolCondition "name" True)))

    it "handles case-insensitive SELECT keyword" $ do
      parseStatement "sEleCt name FROM employees;" `shouldBe` Right (Select [(None, "name")] "employees" Nothing)
        
    it "handles case-insensitive FROM keyword" $ do
      parseStatement "SELECT name frOm employees;" `shouldBe` Right (Select [(None, "name")] "employees" Nothing)

    it "handles case-insensitive WHERE keyword" $ do
      parseStatement "SELECT name FROM employees wHeRe name IS TRUE;" `shouldBe` Right (Select [(None, "name")] "employees" (Just (BoolCondition "name" True)))

    it "handles case-sensitive table names" $ do
      parseStatement "SELECT name FROM Employees;" `shouldBe` Right (Select [(None, "name")] "Employees" Nothing)

    it "handles case-sensitive column names" $ do
      parseStatement "SELECT NAME FROM employees;" `shouldBe` Right (Select [(None, "NAME")] "employees" Nothing)

    it "parses SHOW TABLES statement" $ do
      parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables

    it "parses SHOW TABLE with table name" $ do
      parseStatement "SHOW TABLE employees;" `shouldBe` Right (ShowTable "employees")

    it "returns error for invalid statement" $ do
      parseStatement "INVALID STATEMENT" `shouldSatisfy` isLeft

  describe "Lib2.selectColumns" $ do
    it "handles empty criteria and Left DataFrame" $ do
      selectColumns [] (Left "") `shouldSatisfy` isLeft

    it "filters columns based on criteria" $ do
      selectColumns [(None, "name")] (Right $ snd D.tableEmployees) `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"],[StringValue "Ed"]])

    it "handles non-existent columns" $ do
      selectColumns [(None, "NAME")] (Right $ snd D.tableEmployees) `shouldSatisfy` isLeft

    it "handles invalid DataFrame" $ do
      selectColumns [(None, "Name")] (Right $ DataFrame [] []) `shouldSatisfy` isLeft
