# Scala Spark Data Wrangling Cheat Sheet

Spark's Scala API (from Spark 2.x) functions used to transform dataframes in a similar fashion to Pandas dfs.
This cheatsheet assumes using Databricks, so the set up is taken care of. 

### Setup imports
  
// For implicit conversions like converting RDDs to DataFrames  
import spark.implicits._  



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

### View data

```scala
// Print the schema in a tree format
df.printSchema()

// Displays the content of the DataFrame to stdout  
df.show()  

// Databricks-only function
display(df)
```
  
-----
  
#### Sources
https://spark.apache.org/docs/latest/sql-programming-guide.html#overview  
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html  
