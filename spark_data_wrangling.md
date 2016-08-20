*This cheat sheet is based on the Databricks Spark tutorial course on EdX:*
[BerkeleyX: CS105x Introduction to Apache Spark] (https://www.edx.org/course/introduction-apache-spark-uc-berkeleyx-cs105x)
*The course is offered free of charge*

1. Create SparkContext -> Create SQLContext

#####Create DataFrame  

```python
	sqlContext.createDataFrame()  

	dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))`
````
#####Print column schema  
```python
	dataDF.printSchema()
````
*Spark dataFrames are immutable, so any transformation requires a new dataFrame*

#####Select distinct values  
```python
	newDF = dataDF.distinct().select('*')

	newDF = dataDF.dropDuplicates(['first_name', 'last_name']) # only check for distinct values in the defined cols
````
#####Select acolumns by name and add a new calculated column & drop unwanted columns
```python	
	subDF = dataDF.select('last_name', 'first_name', 'ssn', 'occupation', (dataDF.age - 1).alias('age'))

	subDF = dataDF.drop('occupation')
````

**select() is a transformation**. It returns a new DataFrame that captures both the previous DataFrame and the operation to add to the query (select, in this case). But it does not actually execute anything on the cluster. When transforming DataFrames, we are building up a query plan. That query plan will be optimized, implemented (in terms of RDDs), and executed by Spark only when we call an action.
	
#####View data by using collect()
Only actions such as collect force Spark to optimize and evaluate the transformations defined above.
**WATCH OUT! The output of collect should be small enough to fit in the memory of the driver!** 
```python
	results = subDF.collect()
	print results

# A better way  

	subDF.show(n=30, truncate=False)
````
#####Count rows using count() action
```python	
	dataDF.count()
````
#####Filter rows using filter() transformation and view with show()
```python
	filteredDF = subDF.filter(subDF.age < 10)
	filteredDF.show(truncate=False)
````
#####Check top row first() and n top rows with take()
```python
	filteredDF.first().show() # first row
	filteredDF.take(10).show() # first 10 rows
````
##### Order by 
```python
	dataDF.orderBy(dataDF.last_name.desc()) # sort by last name column in descending order
````
#####Group by + count() / max() / min() / avg()
```python
	dataDF.groupBy('occupation').count().show(truncate=False)
	dataDF.groupBy().avg('age').show(truncate=False)
	dataDF.groupBy().max('age').first()[0]) # find row with max value
	dataDF.groupBy().min('age').first()[0]) # find row with min value
````
#####Sample a subset from the DF
```python
	sampledDF = dataDF.sample(withReplacement=False, fraction=0.10)
````
#####Python Lambda function = Spark user defined function
```python
	less_ten = udf(lambda s: s < 10, BooleanType())
	lambdaDF = subDF.filter(less_ten(subDF.age))
	lambdaDF.show()	
````

#####Add calculated column using .withColumn(colName, col)
```python
	df.withColumn('age2', df.age + 2).collect()
[Row(age=2, name=u'Alice', age2=4), Row(age=5, name=u'Bob', age2=7)]	
````
#####Rename a column using .withColumnRenamed(existing, new)
```python
	df.withColumnRenamed('age', 'age2').collect()
[Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
````

