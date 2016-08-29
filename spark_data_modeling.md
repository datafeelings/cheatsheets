### Data modeling process

#### Summary  
  1. Preprocess variables (data cleaning etc.) and make a DataFrame with them  
  2. Prepare data for modeling: VectorAssembler  
  3. Data modeling  
    3a) Split data into training and test  
    3b) Initialize learner algorithm (e.g. linear regression)  
    3c) Set up the ML pipeline with stages for vectorizer and learner  
    3d) Train the model on the training set    
  
======  
2. Prepare data for modeling: Pass the variables to the VectorAssembler to create a feature vector  

```python
  from pyspark.ml.feature import VectorAssembler

  datasetDF = sqlContext.table("power_plant")

  vectorizer = VectorAssembler()
  vectorizer.setInputCols(["AT", "V", "AP", "RH"])
  vectorizer.setOutputCol("features")
````
3. Data modeling  
3a) Split data into training and test  
Use the randomSplit() method to divide up datasetDF into a trainingSetDF (80% of the input DataFrame) 
and a testSetDF (20% of the input DataFrame), and for reproducibility, use the seed 1800009193L. 
Then cache each DataFrame in memory to maximize performance.  

```python
seed = 1800009193L
(split20DF, split80DF) = datasetDF.randomSplit([0.2, 0.8], seed = seed)

# Let's cache these datasets for performance
testSetDF = split20DF.cache()
trainingSetDF = split80DF.cache()
````  
3b) Initialize learner algorithm (e.g. linear regression)  

```python
# ***** LINEAR REGRESSION MODEL ****

from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml import Pipeline

# Let's initialize our linear regression learner
lr = LinearRegression()
````
