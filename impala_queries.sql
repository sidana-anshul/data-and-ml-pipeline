use anshulalab;

--1.

select t1.emp_no, t1.first_name, t1.last_name, t1.sex, t2.salary
from employees_cap t1
join salaries_cap t2 on t1.emp_no = t2.emp_no limit 50;


select first_name, last_name, hire_date
from employees_cap
where year(hire_date) = 1986 limit 50;

select count(*)
from employees_cap
where year(hire_date) = 1986;



--2.A list showing the manager of each department with the 
--following information: department number, department name, 
--the manager's employee number, last name, first name.



select t1.dept_no, t2.dept_name, t1.emp_no,
t3.last_name, t3.first_name
from dept_manager_cap t1
join departments_cap t2 on t1.dept_no = t2.dept_no
join employees_cap t3 on t1.emp_no = t3.emp_no;

--3.

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name
from employees_cap t1
join dept_emp_cap t2 on t1.emp_no = t2.emp_no
join departments_cap t3 on t2.dept_no = t3.dept_no
limit 50; 


--4.

select first_name, last_name, sex
from employees_cap
where first_name = 'Hercules' and regexp_like(last_name, '^B.*') limit 100;

--5.

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name
from employees_cap t1
join dept_emp_cap t2 on t1.emp_no = t2.emp_no
join departments_cap t3 on t2.dept_no = t3.dept_no
where t3.dept_name = 'Sales' limit 50;

--6.

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name
from employees_cap t1
join dept_emp_cap t2 on t1.emp_no = t2.emp_no
join departments_cap t3 on t2.dept_no = t3.dept_no
where t3.dept_name in ('Sales', 'development') limit 50;

--7.

select last_name, count(last_name) as count_last_name
from employees_cap
group by last_name
order by count_last_name desc
limit 50;

--8.

select t1.salary_bracket, count(t1.salary_bracket) as count_salary
from (
select salary,
case
when mod(salary, 10000) = 0 then concat_ws('-', cast(salary as string), cast(salary+10000-1 as string))
else concat_ws('-', cast((floor(salary/10000)*10000) as string), cast((ceil(salary/10000)*10000)-1 as string))
end as salary_bracket
from salaries_cap ) as t1
group by t1.salary_bracket
order by count_salary desc;




--9.

select t1.emp_title_id, avg(t2.salary) as avg_salary
from employees_cap t1
join salaries_cap t2 on t1.emp_no = t2.emp_no
group by t1.emp_title_id
order by avg_salary desc;

--10.


select t1.tenure_in_years, count(t1.tenure_in_years) as count_emp
from
(select emp_no,
case
when left_company = 1 then (year(last_date) - year(hire_date))
else (2013 - year(hire_date))
end as tenure_in_years
from employees_cap ) as t1
group by t1.tenure_in_years
order by count_emp desc;


--11. Number of male female employees in the company

select sex, count(sex)
from employees_cap
group by sex;

--12. Number of employees per department

select t2.dept_name, count(t2.dept_name)
from dept_emp_cap t1
join departments_cap t2 on t1.dept_no = t2.dept_no
group by t2.dept_name;

--13. Average salary by department

select t3.dept_name, round(avg(t1.salary),2) as avg_dept_salary
from
salaries_cap t1
join dept_emp_cap t2 on t1.emp_no = t2.emp_no
join departments_cap t3 on t2.dept_no = t3.dept_no 
group by t3.dept_name;



--14. average age of employees by sex

select t1.sex, round(avg(t1.age), 2) as avg_age
from
(select sex, (2013-year(birth_date)) as age
from employees_cap) as t1
group by t1.sex;

--15. average salary of employees by sex

select t3.sex, round(avg(t3.salary),2)
from
(select sex, salary
from employees_cap t1
join salaries_cap t2 on t1.emp_no = t2.emp_no) as t3
group by t3.sex;












