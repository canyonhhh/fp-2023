module InMemoryTables
  ( tableEmployees,
    tableInvalid1,
    tableInvalid2,
    tableLongStrings,
    tableWithNulls,
    database,
    TableName,
    employees2
  )
where

import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))

type TableName = String

tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
        [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
      ]
  )

tableInvalid1 :: (TableName, DataFrame)
tableInvalid1 =
  ( "invalid1",
    DataFrame
      [Column "id" IntegerType]
      [ [StringValue "1"]
      ]
  )

tableInvalid2 :: (TableName, DataFrame)
tableInvalid2 =
  ( "invalid2",
    DataFrame
      [Column "id" IntegerType, Column "text" StringType]
      [ [IntegerValue 1, NullValue],
        [IntegerValue 1]
      ]
  )

longString :: Value
longString =
  StringValue $
    unlines
      [ "Lorem ipsum dolor sit amet, mei cu vidisse pertinax repudiandae, pri in velit postulant vituperatoribus.",
        "Est aperiri dolores phaedrum cu, sea dicit evertitur no. No mei euismod dolorem conceptam, ius ne paulo suavitate.",
        "Vim no feugait erroribus neglegentur, cu sed causae aeterno liberavisse,",
        "his facer tantas neglegentur in. Soleat phaedrum pri ad, te velit maiestatis has, sumo erat iriure in mea.",
        "Numquam civibus qui ei, eu has molestiae voluptatibus."
      ]

tableLongStrings :: (TableName, DataFrame)
tableLongStrings =
  ( "long_strings",
    DataFrame
      [Column "text1" StringType, Column "text2" StringType]
      [ [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString]
      ]
  )

tableWithNulls :: (TableName, DataFrame)
tableWithNulls =
  ( "flags",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType]
      [ [StringValue "a", BoolValue True],
        [StringValue "b", BoolValue True],
        [StringValue "b", NullValue],
        [StringValue "b", BoolValue False]
      ]
  )

tableToJoin :: (TableName, DataFrame)
tableToJoin =
  ( "flags2",
    DataFrame
      [Column "another_flag" StringType, Column "other_value" BoolType]
      [ [StringValue "z", BoolValue True],
        [StringValue "b", BoolValue True], [StringValue "g", NullValue],
        [StringValue "a", BoolValue False]
      ]
  )

tableWithNoRows :: (TableName, DataFrame)
tableWithNoRows =
  ( "norows",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType] []
  )

employees2 :: (TableName, DataFrame)
employees2 = ("employees2", DataFrame [Column "id" IntegerType, Column "name" StringType, Column "department" StringType, Column "salary" IntegerType, Column "active" BoolType ] [[IntegerValue 1, StringValue "Alice", StringValue "Engineering", IntegerValue 70000, BoolValue True], [IntegerValue 2, StringValue "Bob", StringValue "Marketing", IntegerValue 60000, BoolValue False], [IntegerValue 3, StringValue "Charlie", StringValue "Sales", IntegerValue 65000, BoolValue True]])

departments :: (TableName, DataFrame)
departments= ("departments", DataFrame [ Column "dept_name" StringType, Column "manager" StringType ] [ [StringValue "Engineering", StringValue "Alice"], [StringValue "Marketing", StringValue "Evan"], [StringValue "Sales", StringValue "Charlie"]])

database :: [(TableName, DataFrame)]
database = [tableEmployees, employees2, departments, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls, tableWithNoRows, tableToJoin]
