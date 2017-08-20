# Scala Spark Data Wrangling Cheat Sheet

Spark's Scala API (from Spark 2.x) functions used to transform dataframes in a similar fashion to Pandas dfs.
This cheatsheet assumes using Databricks, so the set up is taken care of. 

### Setup imports
  
// For implicit conversions like converting RDDs to DataFrames  
import spark.implicits._  



### I/O  
  
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

```

### View data

```scala
// Print the schema in a tree format
df.printSchema()

// Displays the content of the DataFrame to stdout  
df.show()  
```
  
-----
  
#### Sources
https://spark.apache.org/docs/latest/sql-programming-guide.html#overview  
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html  
