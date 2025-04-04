# hive

1. hive (terminal)
2. beeline (terminal) - 
   !connect jdbc:hive2://m02.itversity.com:10000/;auth=noSasl
   username: itv016422
   password: op59g4yxqufa00wj57t56aw72luls1ax

# To set the warehouse path
set hive.metastore.warehouse.dir=/user/itv016422/warehouse;
set hive.metastore.warehouse.dir;

# To avoid the info messages
set hive.server2.logging.operation.level=NONE;

# database creation
create database hive_itv016422; 
use hive_itv016422;

# sample table creation
CREATE TABLE IF NOT EXISTS demo_01(
id INT,
name STRING,
age INT
);

# inserting some sample data
INSERT INTO demo_01 VALUES
(1, 'Ramkrishna', 34),
(2, 'Sneha', 26),
(3, 'Sanidhya', 3);

# quering
SELECT * FROM demo_01;
show tables;

# run hive
hive -f setup_hive_tables.hql

# hive supports three engines
set hive.execution.engine - hive.execution.engine=mr  default
1. mapreduce (mr)
2. spark
3. tez

