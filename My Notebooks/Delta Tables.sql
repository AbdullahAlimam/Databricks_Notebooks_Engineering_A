-- Databricks notebook source
Create TABLE Employees (id INT, name STRING, salary DOUBLE)

-- COMMAND ----------

INSERT into employees
values (1, "ali", 3000),
(2, "abdullah", 5000),
(3, "Momom", 7600),
(4, "Rzan", 3000),
(5, "Zalantah", 2500),
(6, "Jasim", 1500)

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

update employees
set salary = salary + 100
where name like 'a%'

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

select * from employees version as of 2

-- COMMAND ----------

delete from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees to version as of 1

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import random
-- MAGIC
-- MAGIC # Create a Spark session
-- MAGIC spark = SparkSession.builder.appName("InsertExample").getOrCreate()
-- MAGIC
-- MAGIC # Define rondome names
-- MAGIC names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Ivy", "Jack"]
-- MAGIC # Create a DataFrame to hold the data
-- MAGIC data = []
-- MAGIC
-- MAGIC for i in range(1, 101):
-- MAGIC     random_salary = random.randint(1, 10)
-- MAGIC     record = (i, random.choice(names), random_salary * 1000)  # Create a tuple (or any other data structure you prefer)
-- MAGIC     data.append(record)
-- MAGIC
-- MAGIC columns = ["id", "name", "salary"]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Insert the records into the table using a for loop
-- MAGIC query = "INSERT INTO employees (id, name, salary) VALUES "
-- MAGIC # Join all the tuples into a single string
-- MAGIC query += ", ".join([f"({row[0]}, '{row[1]}', {row[2]})" for row in data])
-- MAGIC # Execute the query
-- MAGIC spark.sql(query)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees'))

-- COMMAND ----------

optimize employees
zorder by (ID)

-- COMMAND ----------

------------------------- VACUUM Test ------------------------------

-- COMMAND ----------

CREATE TABLE employees_vacuum (
    id INT,
    name STRING,
    salary DOUBLE
);

-- COMMAND ----------

INSERT into employees_vacuum
values (1, "ali", 3000),
(2, "abdullah", 5000),
(3, "Momom", 7600)


-- COMMAND ----------

Delete from employees_vacuum where id = 2

-- COMMAND ----------

-- should not do this in production
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees_vacuum RETAIN 0 hours -- the deaful is delete files of longer 7 days of life. so nothing will hapen

-- COMMAND ----------

describe history employees_vacuum

-- COMMAND ----------

select * from employees_vacuum@v3

-- COMMAND ----------

vacuum employees -- the deaful is delete files of longer 7 days of life. so nothing will hapen

-- COMMAND ----------

vacuum employees RETAIN 0 hours -- the deaful is delete files of longer 7 days of life. so nothing will hapen

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS DRY RUN; -- the deaful is delete files of longer 7 days of life. so nothing will hapen

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/employees')) 

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees@v2

-- COMMAND ----------

--drop table employees

-- COMMAND ----------

--------------------------------------CTAS-----------------------------------

-- COMMAND ----------

CREATE TABLE employees2
AS SELECT * FROM employees

-- COMMAND ----------

CREATE TABLE employees3_3000
AS SELECT id, name AS full_name FROM employees2 WHERE salary >=3000

-- COMMAND ----------

select * from employees3_3000

-- COMMAND ----------

CREATE TABLE employees4
COMMENT "Contains employees salaries"
PARTITIONED BY (name,salary)
LOCATION 'dbfs:/user/hive/new_databases_location/'
AS SELECT id, name, salary FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/new_databases_location'

-- COMMAND ----------

ALTER TABLE employees ADD CONSTRAINT valid_salary CHECK (salary >= 3000)

-- COMMAND ----------

update employees set salary = 3000 where salary < 3000;
select * from employees

-- COMMAND ----------

ALTER TABLE employees ADD CONSTRAINT valid_salary CHECK (salary >= 3000)

-- COMMAND ----------

insert into employees
values (7,'Zaid', 2999)

-- COMMAND ----------

insert into employees
values (7,'Sumaya', 10000)

-- COMMAND ----------

CREATE TABLE new_employees DEEP CLONE employees;

-- COMMAND ----------

--drop table shallow_employees

-- COMMAND ----------

CREATE TABLE shallow_employees SHALLOW CLONE employees;

-- COMMAND ----------

update employees set salary = 50000 where id = 1

-- COMMAND ----------

select * from shallow_employees where id = 1;

-- COMMAND ----------

select * from employees where id = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Comparison: DEEP CLONE vs SHALLOW CLONE
-- MAGIC
-- MAGIC | Feature               | DEEP CLONE                                             | SHALLOW CLONE                                         |
-- MAGIC |-----------------------|--------------------------------------------------------|-------------------------------------------------------|
-- MAGIC | **Definition**        | Creates a full, independent copy of the source table, including all data files and metadata. | Creates a copy of the source table's metadata but does not duplicate the data files initially. |
-- MAGIC | **Data Duplication**  | Yes, all data files are copied to the new table.       | No, initially references the data files from the source table. |
-- MAGIC | **Metadata Copy**     | Yes, metadata and history are fully copied.            | Yes, metadata is copied, but the actual data is not duplicated initially.  |
-- MAGIC | **Storage Cost**      | Higher, as it duplicates the data files.               | Lower, as it only duplicates the metadata initially.            |
-- MAGIC | **Performance Impact**| Slower, because it needs to copy all data files.       | Faster initially, as it only copies metadata.                   |
-- MAGIC | **Isolation**         | Full isolation from the source table. Changes to the new table do not affect the source table. | Initially not fully isolated. Changes to the data in the new table trigger copy-on-write, isolating changes thereafter. |
-- MAGIC | **Use Cases**         | - Creating an independent backup of a table.<br>- Migrating data to a different environment.<br>- Archiving data while retaining full history. | - Testing with metadata without duplicating data.<br>- Quickly creating a copy of a table for development purposes.<br>- Sharing metadata for collaborative work without duplicating data. |
-- MAGIC | **Example Command**   | `CREATE TABLE new_table DEEP CLONE source_table;`      | `CREATE TABLE new_table SHALLOW CLONE source_table;`  |
-- MAGIC
-- MAGIC ## Use Cases and Scenarios
-- MAGIC
-- MAGIC ### DEEP CLONE
-- MAGIC - **Backup and Recovery**: Creating a full backup of a table to ensure data can be recovered independently.
-- MAGIC - **Data Archival**: Archiving a snapshot of the data, ensuring it remains unchanged and fully accessible.
-- MAGIC - **Data Migration**: Moving a complete copy of the data to a different environment or storage location.
-- MAGIC
-- MAGIC ### SHALLOW CLONE
-- MAGIC - **Development and Testing**: Quickly setting up a test environment that references the same data, without the storage overhead of duplicating the data files.
-- MAGIC - **Collaborative Work**: Sharing the schema and metadata with other teams or projects while pointing to the same underlying data.
-- MAGIC - **Metadata-Only Operations**: Performing operations that require schema and metadata access without needing a full data copy.
-- MAGIC
-- MAGIC ## Summary
-- MAGIC - **DEEP CLONE** is useful when you need a completely independent copy of a table, including all data and metadata, providing full isolation and higher storage costs.
-- MAGIC - **SHALLOW CLONE** is efficient for use cases where you only need a copy of the table's metadata and can work with the same underlying data, offering faster performance and lower storage costs. Initially, data files are shared, but modifications result in independent data files due to the copy-on-write mechanism.
-- MAGIC

-- COMMAND ----------

Show tables

-- COMMAND ----------

CREATE VIEW view_employees
AS select * FROM employees where salary > 4500

-- COMMAND ----------

CREATE Temp VIEW view_employees
AS select * FROM employees

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW view_employees
AS select * FROM employees

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

select * from view_employees -- the temp one that is not connected to a database

-- COMMAND ----------

select * from default.view_employees

-- COMMAND ----------

select * from global_temp.view_employees

-- COMMAND ----------

drop view view_employees

-- COMMAND ----------

drop view view_employees

-- COMMAND ----------

drop view global_temp.view_employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pip install pyspark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.table("employees"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("select * from employees"))
