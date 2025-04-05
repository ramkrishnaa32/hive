"
Query level optimization
    - join optimization
    - filter push down

1. we need to try to avoid joining if possible since it ivolves shuffling of data
2. map side join, when one of the tables is small enough to fit in memory to broadcast to across all mappers
3. bucket map join, when both tables are bucketed on the same column and have the same number of buckets
        -- Both tables are bucketed on the same column and 
        -- Number of buckets in integral multiple of each other
4. sort merge join, when both tables are sorted on the same column
        -- Both tables are bucketed on the same column and 
        -- Both tables are sorted on the same column
        -- Should have the same number of buckets

Datesets:
customers - /user/itv016422/hive_datasets/customers/
orders - /user/itv016422/hive_datasets/orders/
"

USE hive_itv016422;

-- Map Side Join
CREATE EXTERNAL TABLE IF NOT EXISTS customers_external (
    customer_id INT,
    customer_fname STRING,
    customer_lname STRING,
    username STRING,
    password STRING,
    address STRING,
    city STRING,
    state STRING,
    pincode INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/itv016422/hive_datasets/customers/';

CREATE EXTERNAL TABLE IF NOT EXISTS orders_external (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/itv016422/hive_datasets/orders/';

-- Normal join
SELECT o.*, c.*
FROM orders_external o
JOIN customers_external c
ON o.customer_id = c.customer_id
LIMIT 10;

-- It used map side join since one of the table is small enough to fit in memory to broadcast to across all mappers
-- hive.auto.convert.join is set to true by default

-- How it works
-- Before the mapreduce job starts, there is a local process which is executed.
-- The local process reads the small table and creates a hash table in memory.
-- Once the hash table is created, it puts the into hdfs
-- From hdfc its broadcasted to all the nodes in the cluster
-- This called distributed cache
-- Then the mapreduce job starts

-- checking properties:
-- set hive.auto.convert.join=true;
-- set to false, set hive.auto.convert.join=false;

EXPLAIN EXTENDED SELECT o.*, c.*
FROM orders_external o
JOIN customers_external c
ON o.customer_id = c.customer_id
LIMIT 10;


-- Bucket Map Join
CREATE TABLE IF NOT EXISTS customers_bucketed (
    customer_id INT,
    customer_fname STRING,
    customer_lname STRING,
    username STRING,
    password STRING,
    address STRING,
    city STRING,
    state STRING,
    pincode INT
)
CLUSTERED BY (customer_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS orders_b (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO customers_bucketed
SELECT customer_id, customer_fname, customer_lname, username, password, address, city, state, pincode
FROM customers_external;

INSERT INTO orders_b
SELECT order_id, order_date, customer_id, order_status
FROM orders_external;

-- set hive.mapjoin.smalltable.filesize=25000000;
-- set hive.enforce.bucketing=true;
-- set hive.optimize.bucketmapjoin=true;

SELECT o.*, c.*
FROM orders_b o
JOIN customers_bucketed c
ON o.customer_id = c.customer_id
LIMIT 10;

EXPLAIN EXTENDED SELECT o.*, c.*
FROM orders_b o
JOIN customers_bucketed c
ON o.customer_id = c.customer_id
LIMIT 10;


-- Sort Merge Join
CREATE TABLE IF NOT EXISTS customers_sorted (
    customer_id INT,
    customer_fname STRING,
    customer_lname STRING,
    username STRING,
    password STRING,
    address STRING,
    city STRING,
    state STRING,
    pincode INT
)
CLUSTERED BY (customer_id)
SORTED BY (customer_id ASC)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS orders_sorted (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
)
CLUSTERED BY (customer_id)
SORTED BY (customer_id ASC)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- set hive.optimize.bucketmapjoin.sortedmerge=true;
-- set hive.optimize.bucketmapjoin=true;
-- set hive.enforce.bucketing=true;
-- set hive.enforce.sorting=true;
-- set hive.auto.convert.join=true;

INSERT INTO customers_sorted
SELECT customer_id, customer_fname, customer_lname, username, password, address, city, state, pincode
FROM customers_external;

INSERT INTO orders_sorted
SELECT order_id, order_date, customer_id, order_status
FROM orders_external;

SELECT o.*, c.*
FROM orders_sorted o
JOIN customers_sorted c
ON o.customer_id = c.customer_id
LIMIT 10;

EXPLAIN EXTENDED SELECT o.*, c.*
FROM orders_sorted o
JOIN customers_sorted c
ON o.customer_id = c.customer_id
LIMIT 10;
