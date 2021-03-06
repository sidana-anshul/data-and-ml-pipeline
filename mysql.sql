use anabig114214;

drop table if exists salaries_cap;
drop table if exists titles_cap;
drop table if exists employees_cap;
drop table if exists dept_manager_cap;
drop table if exists dept_emp_cap;
drop table if exists departments_cap;


create table salaries_cap(
emp_no int,
salary int);

create table titles_cap(
title_id char(5),
title varchar(30));

create table employees_cap(
emp_no int,
emp_title_id char(5),
birth_date varchar(20),
first_name varchar(30),
last_name varchar(30),
sex char(1),
hire_date varchar(20),
no_of_projects tinyint,
Last_performance_rating varchar(4),
left_company bool,
last_date varchar(20) null);

create table dept_manager_cap(
dept_no char(4),
emp_no int);

create table dept_emp_cap(
emp_no int,
dept_no char(4));

create table departments_cap(
dept_no char(4),
dept_name varchar(30));


load data local infile '/home/anabig114214/capstone_raw_data/departments.csv'
into table departments_cap
fields terminated by ','
enclosed by '"'
ignore 1 rows;

load data local infile '/home/anabig114214/capstone_raw_data/dept_emp.csv'
into table dept_emp_cap
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114214/capstone_raw_data/dept_manager.csv'
into table dept_manager_cap
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114214/capstone_raw_data/employees.csv'
into table employees_cap
fields terminated by ','
optionally enclosed by ''
lines terminated by '\n'
ignore 1 rows
set last_date = str_to_date(last_date, "%m/%d/%Y"), birth_date = str_to_date(birth_date, "%m/%d/%Y"), hire_date = str_to_date(hire_date, "%m/%d/%Y"); 

load data local infile '/home/anabig114214/capstone_raw_data/titles.csv'
into table titles_cap
fields terminated by ','
ignore 1 rows;

load data local infile '/home/anabig114214/capstone_raw_data/salaries.csv'
into table salaries_cap
fields terminated by ','
enclosed by '"'
ignore 1 rows;











