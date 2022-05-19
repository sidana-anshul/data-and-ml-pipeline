use anshulalab;

drop table if exists titles_cap;
create external table titles_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/titles_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/titles_cap.avsc');

drop table if exists dept_emp_cap;
create external table dept_emp_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/dept_emp_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/dept_emp_cap.avsc');

drop table if exists dept_manager_cap;
create external table dept_manager_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/dept_manager_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/dept_manager_cap.avsc');

drop table if exists departments_cap;
create external table departments_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/departments_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/departments_cap.avsc');

drop table if exists salaries_cap;
create external table salaries_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/salaries_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/salaries_cap.avsc');

drop table if exists employees_cap;
create external table employees_cap
stored as avro
location '/user/anabig114214/capstone1_hdfs/data/employees_cap'
tblproperties('avro.schema.url'='/user/anabig114214/capstone1_hdfs/schema/employees_cap.avsc');

