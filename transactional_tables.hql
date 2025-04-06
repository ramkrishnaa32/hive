"
Transactional tables in Hive
//
// -- Transactional tables are used to support ACID (Atomicity, Consistency, Isolation, Durability) properties in Hive.
// -- They allow for INSERT, UPDATE, DELETE operations on tables.
// -- Transactional tables are managed tables that support ACID transactions.
// -- They are created using the TBLPROPERTIES clause with the property 'transactional' set to 'true'.
//
// -- Transactional tables are used for scenarios where data needs to be updated or deleted frequently.
// -- For example, in a data warehouse scenario where data is constantly being updated or deleted.
// -- Transactional tables are also used for scenarios where data needs to be inserted in a transactional manner.
// -- For example, in a scenario where data is being inserted from multiple sources and needs to be consistent.

ACID properties
// -- ACID properties are used to ensure that data is consistent and reliable.
// -- They are used to ensure that data is not lost or corrupted during transactions.

Atomicity:
// -- Atomicity ensures that all operations in a transaction are completed successfully or none at all.
// -- For example, if a transaction involves inserting data into multiple tables, all inserts must be successful or none at all.
// -- If one insert fails, the entire transaction is rolled back.

Consistency:
// -- Consistency ensures that data is in a valid state before and after a transaction.
// -- For example, if a transaction involves updating data in multiple tables, all updates must be consistent.
// -- If one update fails, the entire transaction is rolled back.

Isolation:
// -- Isolation ensures that transactions are executed independently of each other.

Durability:
// -- Durability ensures that once a transaction is committed, it is permanent and cannot be rolled back.
// -- For example, if a transaction involves inserting data into multiple tables, all inserts must be permanent.
// -- If one insert is committed, all inserts are committed.
// -- If one insert fails, the entire transaction is rolled back.

Properties:
set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;

Table Properties:
1. Managed table is only supported for ACID transactions.
2. File format must be ORC.
3. LOAD DATA LOCAL INPATH is not supported.
4. Once table is created as transactional, it cannot be converted to non-transactional.
"

-- Create table
CREATE TABLE IF NOT EXISTS orders_transactional (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

DESCRIBE FORMATTED orders_transactional;

-- Load data
INSERT INTO orders_transactional VALUES (101, "2023-07-25 00:00:00:0", 4568, "COMPLETE");
INSERT INTO orders_transactional VALUES (102, "2023-07-26 00:00:00:0", 4569, "CLOSED");

SELECT * FROM orders_transactional LIMIT 10;

UPDATE orders_transactional
SET order_status = 'PENDING'
WHERE order_id = 101;

SELECT * FROM orders_transactional WHERE order_id = 101;

DELETE FROM orders_transactional
WHERE order_id = 102;

SELECT * FROM orders_transactional LIMIT 10;

SHOW transactions;

-- Automatic compaction
-- Automatic compaction is used to optimize the performance of transactional tables.
-- It is used to merge small files into larger files to improve query performance.
-- It is used to reduce the number of files in a directory to improve query performance.

-- insert only transactional table
-- It supports only insert operations and all kind of file formats.
CREATE TABLE IF NOT EXISTS orders_transactional_insert (
    order_id INT,
    order_date STRING,
    customer_id INT,
    order_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES (
'transactional'='true', 
'transactional_properties'='insert_only');

DESCRIBE FORMATTED orders_transactional_insert;

INSERT INTO orders_transactional_insert VALUES (201, "2023-07-25 00:00:00:0", 4568, "COMPLETE");
INSERT INTO orders_transactional_insert VALUES (202, "2023-07-26 00:00:00:0", 4569, "CLOSED");

SELECT * FROM orders_transactional_insert LIMIT 10;

UPDATE orders_transactional_insert
SET order_status = 'PENDING'
WHERE order_id = 201;

