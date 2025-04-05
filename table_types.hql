/*
Hive table types
    1. Managed table (Data and metadata managed by hive)
    2. External table (Only metadata managed by hive)

Managed table, both data and metadata are deleted while the droping the table
External table, only metadata is deleted while the droping the table
*/

-- Creating managed table
/*
Data will be moved from original location to hive warehouse path give earlier.
When run a insert query it will create another file inside that.
*/

-- Use or create a database
CREATE DATABASE IF NOT EXISTS hive_itv016422;
USE hive_itv016422;

-- Create a managed table
CREATE TABLE IF NOT EXISTS orders_managed (
    order_id integer,
    order_date string,
    customer_id integer,
    order_status string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/itv016422/hive_datasets/orders.csv' INTO TABLE orders_managed; 

-- TO load data from local
-- LOAD DATA LOCAL INPATH '/path/to/orders.csv' INTO TABLE orders_managed;

SELECT * FROM orders_managed LIMIT 10;

INSERT INTO orders_managed VALUES (111101, "2023-07-25 00:00:00:0", 4568, "COMPLETE")

DROP TABLE orders_managed;


-- Creating external table
/*
Data will not be moved from original location to hive warehouse path give earlier.
When run a insert query it will not create another file inside that.
*/
CREATE EXTERNAL TABLE IF NOT EXISTS orders_external (
    order_id integer,
    order_date string,
    customer_id integer,
    order_status string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/itv016422/hive_datasets/orders/';

DESCRIBE FORMATTED orders_external;

SELECT * FROM orders_external LIMIT 10;

INSERT INTO orders_external VALUES (111101, "2023-07-25 00:00:00:0", 4568, "COMPLETE");
SELECT * FROM orders_external WHERE order_id = 111101;

DROP TABLE orders_external;