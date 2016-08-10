# This cheat sheet is based on the Databricks Spark tutorial course on EdX: 
# BerkeleyX: CS105x Introduction to Apache Spark [https://www.edx.org/course/introduction-apache-spark-uc-berkeleyx-cs105x]
# The course is offered free of charge

1. Create SparkContext
2. Create SQLContext
3. Create DataFrame  

	sqlContext.createDataFrame()  

	dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))

4. Print column schema  

	dataDF.printSchema()

# Spark dataFrames are immutable, so any transformation requires a new dataFrame  

5. Select distinct values  

	newDF = dataDF.distinct().select('*')

	newDF = dataDF.dropDuplicates(['first_name', 'last_name']) # only check for distinct values in the defined cols

6. Select acolumns by name and add a new calculated column & drop unwanted columns
	
	subDF = dataDF.select('last_name', 'first_name', 'ssn', 'occupation', (dataDF.age - 1).alias('age'))

	subDF = dataDF.drop('occupation')


# select() is a transformation. It returns a new DataFrame that captures both the previous DataFrame and the operation to add to the query (select, in this case). But it does not actually execute anything on the cluster. When transforming DataFrames, we are building up a query plan. That query plan will be optimized, implemented (in terms of RDDs), and executed by Spark only when we call an action.
	
7. View data by using collect()
Only actions such as collect force Spark to optimize and evaluate the transformations defined above.
WATCH OUT! The output of collect should be small enough to fit in the memory of the driver 

	results = subDF.collect()
	print results

A better way  

	subDF.show(n=30, truncate=False)

8. Count rows using count() action
	
	dataDF.count()

9. Filter rows using filter() transformation and view with show()

	filteredDF = subDF.filter(subDF.age < 10)
	filteredDF.show(truncate=False)

10. Check top row first() and n top rows with take()

	filteredDF.first().show() # first row
	filteredDF.take(10).show() # first 10 rows

11. Order by 

	dataDF.orderBy(dataDF.last_name.desc()) # sort by last name column in descending order

12. Group by + count() / max() / min() / avg()

	dataDF.groupBy('occupation').count().show(truncate=False)
	dataDF.groupBy().avg('age').show(truncate=False)
	dataDF.groupBy().max('age').first()[0]) # find row with max value
	dataDF.groupBy().min('age').first()[0]) # find row with min value

13. Sample a subset from the DF

	sampledDF = dataDF.sample(withReplacement=False, fraction=0.10)

10. Python Lambda function = Spark user defined function

	less_ten = udf(lambda s: s < 10, BooleanType())
	lambdaDF = subDF.filter(less_ten(subDF.age))
	lambdaDF.show()	