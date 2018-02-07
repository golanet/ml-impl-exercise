# ML Implementation Exercise
## General Information:
* Please fork this repository
* This repository holds the basic template of starting a spark session and loading the data.
  - Write your code in Exercise.scala
  - In order to execute your program use `sbt> run file:///PATH/TO/data.csv`
* The dataset is in data.csv file.

## Data Set Information:
* The data is related with direct marketing campaigns.
* It contains 29 features and 1 binary response variable.
* There are 41,187 instances, some of them are categorical, some are textual and the rest are numerical.

## Task:
Develop an algorithm to predict the binary response variable using Random Forest classifier.
Report your algorithm’s predictions quality.

### Steps:
#### Preprocessing:
* Drop features with more than 70% empty values
* Impute missing values, for valid features, with Imputer transformer
#### Feature engineering:
* Apply TF-IDF feature extractor on feature_14
#### Transformation on *categorical* features:
* Apply OneHotEncoder transformer on categorical features
#### Training and test sets:
* Randomly spilt the data into training set (95%) and test set (5%)
#### Train algorithms:
* Apply CrossValidator in order to find the best model
  * set numFolds to 3
  * set setEvaluator to BinaryClassificationEvaluator
  * use the following ParamGrid map
    * randomForest.maxDepth - Array(4, 6, 8)
    * randomForest.numTrees - Array(10, 30, 50)

----
**Required:**
* Solution should be implemented using Spark ML:
  * Pipelines
  * Built-in feature extractors and transformers
  * Built-in ML algorithms
* Make sure to document the steps you take during your work


Good luck
   
 
