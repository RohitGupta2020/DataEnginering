mysql –u username –password   // opening msql shell e


//Creating schema of 6 csv file and loading the data into table

CREATE TABLE department(
            dept_no VARCHAR(10) NOT NULL,
            dept_name VARCHAR(30) NOT NULL
);

load data local infile '/home/anabig114220/department.csv' into table department fields terminated by ',' ignore 1 rows;                                                                                                                                      
    -> ignore 1 rows; 


CREATE TABLE dept_manager (
            dept_no VARCHAR(20) NOT NULL,
            emp_no INT NOT NULL
);

load data local infile '/home/anabig114220/dept_manager.csv' into table dept_manager fields terminated by ',' ignore 1 rows;                                                                                                                                      



CREATE TABLE dept_emp (
            emp_no INT NOT NULL,
            dept_no VARCHAR(10) NOT NULL
);

load data local infile '/home/anabig114220/dept_emp.csv' into table dept_emp fields terminated by ',' ignore 1 rows;                                                                                                                                      

CREATE TABLE title (
            title_id VARCHAR(20) NOT NULL,
            title VARCHAR(20) NOT NULL
);


load data local infile '/home/anabig114220/titles.csv' into table title fields terminated by ',' ignore 1 rows;                                                                                                                                      



CREATE TABLE salaries (
            emp_no BIGINT NOT NULL,
            salary BIGINT NOT NULL
);

load data local infile '/home/anabig114220/salaries.csv' into table salaries fields terminated by ',' ignore 1 rows;                                                                                                                                      

CREATE TABLE employees (
            emp_no Varchar(20) NOT NULL,
            emp_title_id VARCHAR(20) NOT NULL,
            birth_date VARCHAR(20) NOT NULL,
            first_name VARCHAR(20) NOT NULL,
		last_name VARCHAR(20) NOT NULL,
		sex VARCHAR(20) NOT NULL,
		hire_date VARCHAR(20) NOT NULL,
		no_of_projects Int NOT NULL,
		last_performance_rating Varchar(20) NOT NULL,
		left_ Varchar(10) NOT NULL,
		last_date VARCHAR(20) NOT NULL
);

load data local infile '/home/anabig114220/employees.csv' into table employees fields terminated by ',' ignore 1 rows;      



sqoop import-all-tables  --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114220 --username anabig114220 --password Bigdata123 --compression-codec=snappy --as-avrodatafile --warehouse-dir /user/anabig114220/capstone/warehouse --m 1 --driver com.mysql.jdbc.Driver

--- Loading data into HDFS from MySQL with Sqoop command into a root directory as avro format.

--- Creating six external table in hive by for that first move avsc file from local to hdfs.

CREATE EXTERNAL TABLE Salary
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/salaries"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/salaries.avsc');

CREATE EXTERNAL TABLE emp123
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/employees"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/employees.avsc');

CREATE EXTERNAL TABLE depart
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/department"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/department.avsc');

CREATE EXTERNAL TABLE depart_manager
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/dept_manager"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/dept_manager.avsc');

CREATE EXTERNAL TABLE dept_employee
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/dept_emp"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/dept_emp.avsc');

CREATE EXTERNAL TABLE title
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
location "/user/anabig114220/capstone/warehouse/title"
TBLPROPERTIES ('avro.schema.url'='/user/anabig114220/new/title.avsc');
hdfs dfs -rm -r /user/anabig114246/capstone1/department/*

 


---------------------------------------------------------------------------
Hive EDA

1. 
select e.emp_no, e.last_name, e.first_name, 
e.sex, s.salary from emp123 as e join Salary as s on e.emp_no == s.emp_no limit 5;

Output
e.emp_no	e.last_name	e.first_name	e.sex	s.salary
473302	Zallocco	Hideyuki	M	40000
475053	Delgrande	Byong	F	53422
57444	Babb	Berry	F	48973
421786	Verhoeff	Xiong	M	40000
282238	Baumann	Abdelkader	F	40000



2. 

select emp123.first_name, emp123.last_name,emp123.hire_date from emp123 
          where hire_date BETWEEN '1/1/1986' AND '12/31/1986' limit 5 ;

Output
emp123.first_name	emp123.last_name	emp123.hire_date
Xiong	Verhoeff	11/26/1987
Abdelkader	Baumann	1/18/1991
Eran	Cusworth	11/14/1986
Xudong	Samarati	11/13/1985
Lihong	Magliocco	10/23/1993


3.

select depart.dept_no, depart.dept_name, emp123.emp_no,
         emp123.last_name,emp123.first_name from depart join
         depart_manager on depart.dept_no == depart_manager.dept_no
         join emp123 on depart_manager.emp_no == emp123.emp_no limit 5;


Output
depart.dept_no	depart.dept_name	emp123.emp_no	emp123.last_name	emp123.first_name
d001	"Marketing"	110022	Markovitch	Margareta
d001	"Marketing"	110039	Minakawa	Vishwani
d002	"Finance"	110085	Alpin	Ebru
d002	"Finance"	110114	Legleitner	Isamu
d003	"Human Resources"	110183	Ossenbruggen	Shirish

4

 select depart.dept_name, emp123.emp_no,
         emp123.last_name,emp123.first_name from depart join
         depart_manager on depart.dept_no == depart_manager.dept_no
         join emp123 on depart_manager.emp_no == emp123.emp_no limit 5;  


Output
depart.dept_name	emp123.emp_no	emp123.last_name	emp123.first_name
"Marketing"	110022	Markovitch	Margareta
"Marketing"	110039	Minakawa	Vishwani
"Finance"	110085	Alpin	Ebru
"Finance"	110114	Legleitner	Isamu
"Human Resources"	110183	Ossenbruggen	Shirish

5.
 

select emp123.first_name,emp123.last_name,emp123.sex
         from emp123 where first_name = 'Hercules' and last_name like 'B%' limit 5;
78354

6
select emp123.emp_no, emp123.last_name,emp123.first_name,depart.dept_name
        from dept_employee join emp123 on dept_employee.emp_no == emp123.emp_no
        join depart on dept_employee.dept_no == depart.dept_no 
            where depart.dept_name = 'Sales'

7.

select emp123.emp_no, emp123.last_name,emp123.first_name, depart.dept_name from dept_employee
          join emp123 on dept_employee.emp_no= emp123.emp_no join depart
          on dept_employee.dept_no = depart.dept_no where depart.dept_name = 'Sales'
          or depart.dept_name= 'Development' limit 5;

8.


Select last_name, count(emp_no) from emp123 group by last_name order by count(emp_no) Desc;









