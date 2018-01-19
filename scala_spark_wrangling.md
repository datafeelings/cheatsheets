# Scala Spark Data Wrangling Cheat Sheet

Spark's Scala API (from Spark 2.x) functions used to transform dataframes in a similar fashion to Pandas dfs.
This cheatsheet assumes using Databricks, so the set up is taken care of. 

### Setup imports  
  
```scala
import spark.implicits._ // For implicit conversions like converting RDDs to DataFrames
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._ // for things like Rank, UDF
import org.apache.spark.sql.expressions.Window


```

### I/O  

For built-in types, their short names can be used for read and write operations:   
`json, parquet, jdbc, orc, libsvm, csv, text`  
    
**Read Parquet into DF**  

```scala
val df = spark.read
  .load("examples/src/main/resources/users.parquet")
```

**Read CSV into DF**  
```scala

val df = spark.read  
  .option("header", "true")  
  .csv("path/filename.csv")

```

**Read JSON into DF**  
```scala

val df = spark.read  
  .json("path/filename.json")  

// alternatively
val df = spark.read
  .format("json")
  .load("path/filename.json")  

```

**Write into file**  

```scala
df.select("column_this", "column_that").write.save("df.parquet")
// with options
df.select("column_this", "column_that").write.format("parquet").save("df.parquet")

// with more options for CSV
// source: https://forums.databricks.com/questions/8515/datawriteformatcomdatabrickssparkcsv-added-additio.html

dataframe
    .write
    .option("header", true)
    .option("quote", "\u0000") //deal with unicode quoting
    .csv("/FileStore/temp.csv")

```

**Write into table**

```scala
df
  .write
  .sortBy("column") // optional
  .partitionBy("column") // optional
  .mode("overwrite) // option to enable overwrite, by default an Exception will be thrown if file/table exists
  .format("parquet").saveAsTable("df_table")
```

### View data

```scala
// Print the schema in a tree format
df.printSchema()

// Displays the content of the DataFrame to stdout  
df.show()  

// Take first n rows (.first() for first row only)
df.head(5)

// Returns all column names and their data types as an array.
df.dtypes

// Returns all column names as an array.
df.columns

// Databricks-only function
display(df)
```

### Selection & Setting Values

**Getting by position or name**  
If you want to get just one element by name or position index, you have to specify its type and apply a map function on each row in order to extract it. Then out of the extracted Array pick the required element by position

```scala
df.map(r=>r.getAs[String](1)).collect()(20) // here, the element at position 20 is taken from column 1 typed as String

// column names are also supported
df.map(r=>r.getAs[String]("zip")).collect()(20) // here, the element at position 20 is taken from column "zip" typed as String
```
  
**Selecting columns**  
  
```scala
df.select("*")
df.select("columnthis")
df.selectExpr("int(columnthis) as int_columnthis") // convert to a different type and rename
df.selectExpr("*", "int(columnthis) as int_columnthis") // same as above, but also select all of the original columns 
```
  
**Selecting values by Boolean (filtering the DF)**  
  
```scala
df.filter("columnthis < 5") 
df.filter("columnthis < 5 AND columnthat = 'stringvalue' OR columnthh IS NULL") // familiar SQL syntax
```
In order to produce a DF with Boolean values filled in for the result of the comparison, use `selectExpr` instead:
  
```scala
df.selectExpr("columnthis < 5") 
```

**Setting values by Boolean condition**
  
As DFs are immutale, a new DF with the updated values should be returned

```scala
// Function to update the values based on a condition
val update_func = (when($"update_col" === replace_val, new_value).otherwise($"update_col"))

val df1 = df.withColumn("new_column_name", update_func)
```
  
### Dropping and NAs  
  
**Count empty values and NAs**  

```scala
// "" deals with empty Strings, Null with missing Strings, and isNaN with numeric NAs
df.filter(df(colName).isNull || df(colName) === "" || df(colName).isNaN).count().show()

// in order to get the split into NA / non NA:
df.groupBy($"colName".isNull).count().show()
```

**Drop columns & duplicates**  

```scala
df.drop("columnthis","columnthat")  

df.distinct // Returns a new DataFrame that contains only the unique rows from this DataFrame

df.dropDuplicates()
df.dropDuplicates("columnthis") // will only return rows with unique values in the "columnthis" column

df.na.drop() // Dropping rows containing any null values.

```
  
**Fill NA**  
The key of the map is the column name, and the value of the map is the replacement value. The value must be of the following type: `Int, Long, Float, Double, String`.
  
For example, the following replaces `null` values in column "A" with string "unknown", and `null` values in column "B" with numeric value 1.0.

```scala
df.na.fill(Map(
 "A" -> "unknown",
 "B" -> 1.0
))
```

### Sort & Rank

**Order by values**  

```scala
df.orderBy("sortcol") // default is ascending
df.orderBy($"col1", $"col2".desc)
```
  
**Assign rank by column values**  

```scala
// this creates a window for each value over which the rank is applied
df.select($"columnthis", rank.over(Window.orderBy($"columnthis")).alias("rank")).show() 

// another option is to use SQL
df.selectExpr("columnthis", "RANK() OVER (ORDER BY columnthis) AS rank").show()
```
  
### Retrieving DataFrame Information

```scala
df.count()   // Returns the number of rows in the DataFrame.

df.describe() // Computes statistics for numeric columns, including count, mean, stddev, min, and max.
df.describe("column1", "column2")

// There is some problem with the syntax below even though it is listed here:
// https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/GroupedData.html
// df.agg(...) is a shorthand for df.groupBy().agg(...)
// df.agg(max($"age"), avg($"salary")) // apply aggregation functions w/o grouping to get to min/max/avg values of columns
// df.groupBy().agg(max($"age"), avg($"salary"))
```

### Applying Functions

**Applying function to a column**

Spark UDFs are *column-based* functions and can be simple unnamed functions or more complex functions.  
Simple case:  

```scala
// define the UDF
val upperUDF = udf { s: String => s.toUpperCase } // warning - this function can't handle Nulls

// or define the UDF and register it be available to the SQLContext:
spark.udf.register("upperUDF",  (s: String) => s.toUpperCase)

// apply the UDF to create a new column
df.withColumn("upper", upperUDF($"columnname")).show()

// or like this if the UDF is registered:
df.selectExpr("*", "upperUDF(columnname) as udf_output").show()
```

### Extract values from Dataframe

**Get column values as a simple array**

```scala
// Will return the first 5 values of colName as a simple Array[String]
df.select("colName").take(5).map(x => x.getString(0))
```

-----
  
#### Sources
https://spark.apache.org/docs/latest/sql-programming-guide.html#overview  
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html  
https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/DataFrame.html  
https://stackoverflow.com/questions/33729787/computing-rank-of-a-row  
https://stackoverflow.com/questions/44329398/count-empty-values-in-dataframe-column-in-spark-scala  
https://stackoverflow.com/questions/29109916/updating-a-dataframe-column-in-spark  
