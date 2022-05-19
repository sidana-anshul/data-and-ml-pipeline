#run mysql.sql which creates the required mysql tables and loads the data

mysql -u anabig114214 -pBigdata123 anabig114214 < capstone1/mysql.sql

#before transferring data from mysql to hdfs using sqoop, make sure that the target directories are empty

hdfs dfs -rm -r /user/anabig114214/capstone1_hdfs/data/*
hdfs dfs -rm -r /user/anabig114214/capstone1_hdfs/schema/*

#sqoop commands to transfer data from mysql to hdfs in avro format

sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table titles_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data
sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table salaries_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data
sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table dept_emp_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data
sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table departments_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data
sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table dept_manager_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data
sqoop import -Dmapreduce.job.user.classpath.first=true --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114214 --username anabig114214 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --table employees_cap --m 1 --driver com.mysql.jdbc.Driver --warehouse-dir /user/anabig114214/capstone1_hdfs/data

#transfer the avro schema created for each file to hdfs

hdfs dfs -put titles_cap.avsc /user/anabig114214/capstone1_hdfs/schema/
hdfs dfs -put salaries_cap.avsc /user/anabig114214/capstone1_hdfs/schema/
hdfs dfs -put departments_cap.avsc /user/anabig114214/capstone1_hdfs/schema/
hdfs dfs -put dept_emp_cap.avsc /user/anabig114214/capstone1_hdfs/schema/
hdfs dfs -put dept_manager_cap.avsc /user/anabig114214/capstone1_hdfs/schema/
hdfs dfs -put employees_cap.avsc /user/anabig114214/capstone1_hdfs/schema/

#remove the avro schema files and map reduce programs created by sqoop

rm *.avsc
rm *.java

#create hive tables using the transferred data

hive -f capstone1/hive.sql


#fetch exploratory data analysis results from impala queries

impala-shell -i ip-10-1-2-103.ap-south-1.compute.internal -d default -f capstone1/impala_queries.sql -o impala_results
.txt


#create the ml model using pyspark

spark-submit ML_pipeline.py

