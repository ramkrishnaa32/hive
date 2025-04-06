"""
Hive integration with Spark
It also includes a function to create a Spark session with Hive support.
It includes the necessary configurations for connecting to a Hive metastore.
"""

import getpass
from pyspark.sql import SparkSession

def initialize_spark_session(AppName):
    """
    Initializes and returns a Spark session with predefined configurations.
    Parameters:
        AppName (str): The name of the Spark application.
    Returns:
        SparkSession: A configured Spark session.
    """
    username = getpass.getuser()
    spark = SparkSession. \
    builder. \
    appName(AppName). \
    config('spark.ui.port', '0'). \
    config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", f'/Users/{username}/warehouse'). \
    enableHiveSupport(). \
    master('local'). \
    getOrCreate()
    return spark

spark = initialize_spark_session("Hive Integration")

spark.sql("SHOW DATABASES").show()
spark.sql("CREATE DATABASE IF NOT EXISTS hive_db")
spark.sql("USE hive_db")
spark.sql("SHOW TABLES").show()

spark.sql("CREATE TABLE IF NOT EXISTS hive_table (id INT, name STRING) USING hive")
spark.sql("INSERT INTO hive_table VALUES (1, 'Alice'), (2, 'Bob')")
spark.sql("SELECT * FROM hive_table").show()

query = """
CREATE EXTERNAL TABLE IF NOT EXISTS orders_external (
    order_id integer,
    order_date string,
    customer_id integer,
    order_status string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/Users/kramkrishnaachary/Learning/data_engineering/hive_datasets/orders';
"""
spark.sql(query)
spark.sql("SELECT * FROM orders_external").show(20, False)

