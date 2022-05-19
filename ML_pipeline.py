
## import the required modules

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator



## initiate SparkSession

spark = (SparkSession.builder.appName("app_name") \
	.config("hive.metastore.uris","thrift://ip-10-1-2-24.ap-south-1.compute.internal:9083") \
	.enableHiveSupport().getOrCreate())

## load the required datasets from the hive warehouse

employees_df = spark.table('anshulalab.employees_cap')
salaries_df = spark.table('anshulalab.salaries_cap')
departments_df = spark.table('anshulalab.departments_cap')
titles_df = spark.table('anshulalab.titles_cap')
dept_manager_df = spark.table('anshulalab.dept_manager_cap')
dept_emp_df = spark.table('anshulalab.dept_emp_cap')

## dept_emp_df dataframe has duplicate employee numbers, remove those duplicates

dept_emp_df_clean = dept_emp_df.dropDuplicates(['emp_no'])

## join the required datasets. Since the required features are only in employees_df, salaries_df
## and dept_emp_df_clean, only these datasets are joined

employees_df_joined = employees_df.join(salaries_df, 'emp_no').join(dept_emp_df_clean, 'emp_no')

## create features: age of employee and tenure of employee

employees_df_joined = employees_df_joined.withColumn('employee_age', 2013-year('birth_date'))

employees_df_joined = employees_df_joined.withColumn('tenure', when(col('left_company') == 1,\
                                                                    (year('last_date')-year('hire_date'))) \
                                                                     .otherwise((2013 - year('hire_date'))))

## change the name of the Y variable to label and change its type from boolean to integerType

employees_df_joined = employees_df_joined.withColumnRenamed('left_company', 'label')

employees_df_joined = employees_df_joined.withColumn('label', employees_df_joined.label.cast(IntegerType()))

## split the dataset into train_df and test_df

train_df, test_df = employees_df_joined.randomSplit([0.7, 0.3], seed=42)


## define the continuous and categorical features

continuous_features = ['no_of_projects', 'salary', 'employee_age', 'tenure']

categorical_features = ['emp_title_id', 'sex', 'dept_no', 'last_performance_rating']

## create the string indexers for categorical features

SI_emp_title_id = StringIndexer(inputCol='emp_title_id', outputCol='emp_title_id_idx')
SI_sex = StringIndexer(inputCol='sex', outputCol='sex_idx')
SI_dept_no = StringIndexer(inputCol='dept_no', outputCol='dept_no_idx')
SI_last_performance = StringIndexer(inputCol='last_performance_rating', outputCol='last_performance_rating_idx')

## create the OneHotEncoders

OHE_emp_title_id = OneHotEncoder(inputCol='emp_title_id_idx', outputCol='emp_title_id_vec')
OHE_sex = OneHotEncoder(inputCol='sex_idx', outputCol='sex_vec')
OHE_dept_no = OneHotEncoder(inputCol='dept_no_idx', outputCol='dept_no_vec')
OHE_last_performance = OneHotEncoder(inputCol='last_performance_rating_idx', outputCol='last_performance_rating_vec')

## feature columns after output from OneHotEncoders

feature_cols = ['emp_title_id_vec', 'sex_vec', 'dept_no_vec', 'last_performance_rating_vec'] + continuous_features

## create the vector assembler

assembler = VectorAssembler(inputCols=feature_cols, outputCol = 'features')

## create the RandomClassifier object

rf = RandomForestClassifier(labelCol = 'label', featuresCol = 'features')

## define the ML pipeline

pipeline = Pipeline(stages = [SI_emp_title_id, SI_sex, SI_dept_no, 
 SI_last_performance, OHE_emp_title_id, OHE_sex, OHE_dept_no,
 OHE_last_performance, assembler, rf])

## fit the train dataset to the pipeline and create the model

model = pipeline.fit(train_df)

## make predictions on both train and test datasets

pred_train = model.transform(train_df)
pred_test = model.transform(test_df)


## function to evaluate models

def evaluate_model(pred):
    
    eval_accuracy = (MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy"))
  
    eval_precision = (MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision"))
  
    eval_recall = (MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall"))
  
    eval_f1 = (MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1"))

    accuracy = eval_accuracy.evaluate(pred)

    precision =  eval_precision.evaluate(pred)

    recall =  eval_recall.evaluate(pred)

    f1 =  eval_f1.evaluate(pred)

    print(f"""
    Accuracy  = {accuracy}
    Error     = {1-accuracy}
    Precision = {precision}
    Recall    = {recall}
    F1        = {f1}""")

print("Evaluation metrics - Training dataframe")
evaluate_model(pred_test)

print("Evaluation metrics - Test dataframe")
evaluate_model(pred_train)






