/*
1. Table structure level optimization
    - Partitioning
    - Bucketing
2. Query level optimization
    - join optimization
    - filter push down

Partitioning:
    Partitioning divides the table into smaller parts based on the partition column.
    This is useful when we have a large table and we want to query only a subset of the data.
    For example, if we have a table of sales data, we might partition it by year and month.
    This way, when you query for sales data for a specific month, Hive only needs to scan the relevant partition.

Bucketing:
    Bucketing divides the data into a fixed number of buckets based on the hash value of the bucket column.
    This is useful when you want to perform joins on large tables.
    For example, if you have a table of customers and a table of orders, you might bucket both tables by customer_id.
    This way, when you join the two tables, Hive can read only the relevant buckets instead of scanning the entire table.
    Partitioning and bucketing can be used together to further optimize query performance
*/

-- Partitioning
CREATE TABLE IF NOT EXISTS orders_partitioned (
    order_id integer,
    order_date string,
    customer_id integer
) PARTITIONED BY (order_status string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- set properties before inserting data
-- set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO orders_partitioned PARTITION (order_status)
SELECT order_id, order_date, customer_id, order_status
FROM orders_managed;

-- Verify the data in hdfs
-- hadoop fs -ls /user/itv016422/warehouse/hive_itv016422.db/orders_partitioned

-- see the partitions created
SHOW PARTITIONS orders_partitioned;

-- Querying the partitioned table
DESCRIBE FORMATTED orders_partitioned;
SELECT * FROM orders_partitioned WHERE order_status = 'COMPLETE' LIMIT 10;

-- Querying the partitioned table with filter push down
EXPLAIN EXTENDED SELECT * FROM orders_partitioned WHERE order_status = 'COMPLETE' LIMIT 10;