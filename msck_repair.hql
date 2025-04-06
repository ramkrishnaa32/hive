"
MSCK Repair
// This script is used to repair the metadata of a Hive table in a Hadoop environment.
// It is used to add partitions to a table that are present in the underlying data but not in the Hive metastore.
"

CREATE EXTERNAL TABLE IF NOT EXISTS orders_p (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
) PARTITIONED BY (order_status STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/itv016422/hive_datasets/orders_p/';

SHOW PARTITIONS orders_p;

-- currently there is no partitions, and there is no data in the location
-- hadoop fs -ls /user/itv016422/hive_datasets/orders_p/

-- Load data into the table
INSERT INTO orders_p PARTITION (order_status)
SELECT order_id, order_date, customer_id, order_status
FROM orders_external;

-- Verify the data in hdfs
-- hadoop fs -ls /user/itv016422/warehouse/hive_itv016422.db/orders_p/

-- Now, we will add partitions to hsdfs
-- hadoop fs -mkdir /user/itv016422/warehouse/hive_itv016422.db/orders_p/order_status=COMPLETE
-- haddop fs -put /home/itv016422/hive_datasets/orders_p/COMPLETE/* /user/itv016422/warehouse/hive_itv016422.db/orders_p/order_status=COMPLETE/

-- now, we will add partitions to the table
MSCK REPAIR TABLE orders_p;

-- verify the data in the table
SHOW PARTITIONS orders_p;
SELECT * FROM orders_p WHERE order_status = 'COMPLETE' LIMIT 10;