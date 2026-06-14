# local_test_setup.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark with Delta Support
spark = SparkSession.builder \
    .appName("LocalDQSetup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Create the DQ Metrics Database and Rules Table
spark.sql("CREATE DATABASE IF NOT EXISTS dq_metrics_trn")

rules_data = [
    ("parts_db", "battery_table", "PART_NO", None, "part_not_null",
     ".isComplete('PART_NO')", "Error", "Part number is missing", "Y", "verify", "N")
]

rules_columns = ["db_name", "table_name", "column_name", "sql", "constraint_name",
                 "constraint_rule", "check_level", "check_message", "is_active", "rule_type", "reject_capture"]

spark.createDataFrame(rules_data, rules_columns) \
    .write.format("delta").mode("overwrite").saveAsTable("dq_metrics_trn.data_quality_manual_rules")

# 2. Create the Dummy Source Data Table to be tested
spark.sql("CREATE DATABASE IF NOT EXISTS parts_db")

source_data = [
    (101, "BATT-001", "SupplierA"),
    (102, None, "SupplierB"),  # This row should fail the "isComplete" rule
    (103, "BATT-002", "SupplierA")
]

source_columns = ["id", "PART_NO", "SUPPLIER"]

spark.createDataFrame(source_data, source_columns) \
    .write.format("delta").mode("overwrite").saveAsTable("parts_db.battery_table")

print("Local Environment Setup Complete!")
