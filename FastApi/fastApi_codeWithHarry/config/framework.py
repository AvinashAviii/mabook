# bcp_transform_load.py

import json
import logging
from pyspark.sql import SparkSession
from pyhocon import ConfigFactory #to read config and parse it

# ----------------------------
# Logger Setup
# ----------------------------
logging.basicConfig(level=logging.INFO) #shows logs of different level
logger = logging.getLogger("BCP-TRANSFORM") #create a named logger instance

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder.appName("BcpTransformLoadPy").getOrCreate() # create one spark session

# ----------------------------
# Load Config from S3
# ----------------------------
def load_config(path: str):
    logger.info(f"Loading config from: {path}")#log for loading ccnfig from s3
    text = "\n".join(spark.read.text(path).rdd.map(lambda r: r[0]).collect()) #read file from config ,Reads file as a DataFrame,Each line = one row,convert to rdd,extract text from each row,bring all data to driver
    return ConfigFactory.parse_string(text) #Converts string → structured config object

# ----------------------------
# Validate Config
# ----------------------------
def validate_config(table_conf, table_name):
    if "settings" not in table_conf:
        raise Exception(f"[{table_name}] Missing settings block")

    if "queries" not in table_conf:
        raise Exception(f"[{table_name}] Missing queries block")

    if "final_dfview" not in table_conf["queries"]:
        raise Exception(f"[{table_name}] final_dfview is required")

# ----------------------------
# Execute Query Pipeline
# ----------------------------
def execute_queries(queries: dict):
    temp_views = {}#dict to store key(view) and value(query)

    for view_name, query in queries.items():
        logger.info(f"Executing view: {view_name}")

        df = spark.sql(query)#will take query
        df.createOrReplaceTempView(view_name)#will take view

        temp_views[view_name] = df #Keeps reference of DataFrame in dictionary

    return temp_views

# ----------------------------
# Write Logic (Insert / Merge)
# ----------------------------
def write_to_target(final_df, settings, table_name, trn_db):
    target_table = f"{trn_db}.{table_name}" #target table final
    load_type = settings.get("loadType", "ins")#read config
    merge_keys = settings.get("mergeKey", [])#read config

    logger.info(f"Writing to target: {target_table} with loadType={load_type}")#log message

    if load_type == "ins":#if load type is insert , then append no duplicate checks
        final_df.write.mode("append").format("delta").saveAsTable(target_table)

    elif load_type == "merge":#if load type is merge , and merge key not given raise error
        if not merge_keys:
            raise Exception(f"{table_name}: mergeKey required for merge load")

        # Create temp view
        final_df.createOrReplaceTempView("source_data")#Makes your DataFrame usable in SQL

        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys]) #t.id = s.id AND t.date = s.date

        merge_sql = f"""
        MERGE INTO {target_table} t
        USING source_data s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
#This is called UPSERT (Update + Insert)
        logger.info(f"Executing MERGE for {table_name}")
        spark.sql(merge_sql)

    else:
        raise Exception(f"Unsupported loadType: {load_type}") #Invalid config

# ----------------------------
# Process Single Table #👉 Reads config for one table
# 👉 Runs all transformation queries
# 👉 Builds final DataFrame
# 👉 Loads it into target table
# ----------------------------
def process_table(config, table_name, raw_db, trn_db):
    logger.info(f"Processing table: {table_name}")

    table_conf = config["trnconfig"][table_name] #Extract table-specific config

    validate_config(table_conf, table_name) #Ensures:required fields exist,no missing queries/settings

    settings = table_conf["settings"]
    queries = table_conf["queries"]

    try:
        # Step 1: Use RAW DB,Ensures queries read from correct source DB
        spark.sql(f"USE {raw_db}")

        # Step 2: Execute all queriesThis:
        #
        # Runs all SQL queries
        # Creates temp views
        # Returns dictionary of DataFrames
        temp_views = execute_queries(queries)

        # Step 3: Final DF
        final_df = temp_views["final_dfview"] #This is the final output of transformation pipeline

        logger.info(f"Final DF count for {table_name}: {final_df.count()}") #data count

        # Step 4: Write to TRN table,Writes data using:
        #
        # append OR
        # merge (upsert)
        write_to_target(final_df, settings, table_name, trn_db)

        logger.info(f"Successfully processed {table_name}")

    except Exception as e: #Logs error,if any exception
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise

# ----------------------------
# Main Driver
#
#👉 Accepts runtime input (JSON string)
#👉 Extracts environment + table details
#👉 Loads config
#👉 Runs ETL for each table
# ----------------------------
def main(params_json: str):
    logger.info("Starting BCP Transform Framework") #Marks beginning of job

    params = json.loads(params_json)#Converts JSON string → Python dictionary

    env = params.get("env")#Extract parameters
    raw_db = params.get("rawlayerdb")
    trn_db = params.get("trnlayerdb")
    tables = params.get("table_list").split(",")
    config_path = params.get("configpath")

    logger.info(f"Environment: {env}")#Log environment & tables
    logger.info(f"Tables: {tables}")

    # Load Config
    config = load_config(config_path)

    # Process Each Table,Read its config
    # Execute SQL transformations
    # Generate final DataFrame
    # Write to target table
    for table in tables:
        process_table(config, table.strip(), raw_db, trn_db) #table.strip() ->Removes spaces:

    logger.info("All tables processed successfully")

# ----------------------------
# Entry Point (Databricks)
# ----------------------------
if __name__ == "__main__": #Run this block only when script is executed directly
    #This block is the execution entry point for your script in Databricks.
    #Runs your pipeline only when the script is executed directly
#👉 Reads runtime input from Databricks widgets
#👉 Calls your main() function
#👉 Handles and logs failures
    try:
        # Databricks widget input.dbutils.widgets is a Databricks utility
        #
        # 👉 It allows you to:
        #
        # Pass parameters to notebooks/jobs
        args = dbutils.widgets.get("args") #This entire JSON string becomes:args
        main(args)#Call main function
        #args → parse JSON → load config → process tables → write data

    except Exception as e:#What happens:,Logs error message,Re-throws exception
        logger.error(f"Job Failed: {str(e)}")
        raise