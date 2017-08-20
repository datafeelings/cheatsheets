# Scala Spark Data Wrangling Cheat Sheet

Spark's Scala API (from Spark 2.x) functions used to transform dataframes in a similar fashion to Pandas dfs.
This cheatsheet assumes using Databricks, so the set up is taken care of. 

### Setup imports
  
// For implicit conversions like converting RDDs to DataFrames  
```scala
import spark.implicits._
import org.apache.spark.sql.Row

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
    .option("quote", "\u0000") //magic is happening here
    .csv("/FileStore/temp.csv")

```

**Write into table**

```scala
df
  .write
  .sortBy("column") // optional
  .partitionBy("column") // optional
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

### Selection

**Getting by position or name**
If you want to get just one element by name or position index, you have to specify its type and apply a map function on each row in order to extract it. Then out of the extracted Array pick the required element by position

```scala
df.map(r=>r.getAs[String](1)).collect()(20) // here, the element at position 20 is taken from column 1 typed as String

// column names are also supported
df.map(r=>r.getAs[String]("zip")).collect()(20) // here, the element at position 20 is taken from column "zip" typed as String

```
**Getting by boolean**


-----
  
#### Sources
https://spark.apache.org/docs/latest/sql-programming-guide.html#overview  
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html  
https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/DataFrame.html  
