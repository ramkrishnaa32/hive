-- Use or create a database
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- Create a managed table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    customer_fname STRING,
    customer_lname STRING,
    username STRING,
    password STRING,
    address STRING,
    city STRING,
    state STRING,
    pincode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Load data into the managed table
LOAD DATA LOCAL INPATH '/path/to/customers.csv' INTO TABLE customers;

-- Create an external table
CREATE EXTERNAL TABLE IF NOT EXISTS products (
    product_id INT,
    name STRING,
    category STRING,
    price DECIMAL(10,2),
    stock_quantity INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive/external/products/';

-- Verify tables
SELECT * FROM customers LIMIT 10;
SELECT * FROM products LIMIT 10;
