# data-and-ml-pipeline

An end-to-end implementation of a data and ml pipeline which covers the following steps

1. We have been given employees data that is arranged in 6 separate csv files
2. First step is to create a mysql database and load the csv files into the database. The code for this task is compiled in mysql.sql
3. Next this data is transferred from mysql database to hdfs in avro format using sqoop. The code is compiled in data_ml_pipeline.sh
4. Using the avro data and schemas of transferred data, a hive database is created. The code can be found in hive.sql
5. Exploratory data analysis using impala. The code is compiled in impala_queries.sql
6. Exploratory data anysis using spark sql.
7. Creating an ML pipeline that reads data from hive tables and uses the random forest classifier algorithm to predict where an employee will leave the organization
   or not. The code is compiled in ML_pipeline.py
   
This entire code is compiled in shell file data-ml-pipeline.sh which runs the entire data and ml pipeline in one go.
