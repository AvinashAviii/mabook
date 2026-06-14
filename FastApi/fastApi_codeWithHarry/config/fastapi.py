# api_ingestion_service.py

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyhocon import ConfigFactory
import boto3
import logging
import traceback

# ----------------------------
# Logger
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BCP-INGESTION-API")

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder.appName("BCPFileBasedIngestionAPI").getOrCreate()

# ----------------------------
# FastAPI App
# ----------------------------
app = FastAPI(title="BCP Ingestion API")

# ----------------------------
# Request Model
# ----------------------------
class IngestionRequest(BaseModel):
    env: str
    config_path: str

# ----------------------------
# Load Config
# ----------------------------
def load_config(path):
    txt = "\n".join(spark.read.text(path).rdd.map(lambda r: r[0]).collect())
    return ConfigFactory.parse_string(txt)

# ----------------------------
# Schema Parsing
# ----------------------------
def get_field_struct(source_schema, target_schema=None):

    def parse_schema(schema_str):
        fields = []
        for field in schema_str.split(","):
            name, dtype = field.strip().split(" ")
            dtype = dtype.lower()

            type_map = {
                "string": StringType(),
                "double": DoubleType(),
                "int": IntegerType(),
                "date": DateType(),
                "number": LongType(),
                "timestamp": TimestampType()
            }

            if dtype not in type_map:
                raise Exception(f"Unsupported datatype: {dtype}")

            fields.append(StructField(name, type_map[dtype], True))

        return fields

    source_fields = parse_schema(source_schema)
    target_fields = parse_schema(target_schema) if target_schema else None

    return source_fields, target_fields

# ----------------------------
# Schema Apply
# ----------------------------
def apply_original_schema(df, schema):
    cols = []
    for field in schema.fields:
        if field.name in df.columns:
            cols.append(col(field.name).cast(field.dataType).alias(field.name))
        else:
            cols.append(lit(None).cast(field.dataType).alias(field.name))
    return df.select(*cols)

# ----------------------------
# Column Mapping
# ----------------------------
def transform_with_mapping(df, source_fields, target_fields):

    if target_fields:
        mapping = dict(zip(
            [f.name for f in source_fields],
            [f.name for f in target_fields]
        ))

        logger.info(f"Column Mapping: {mapping}")

        return df.select([col(c).alias(mapping.get(c, c)) for c in mapping.keys()])

    return df

# ----------------------------
# Create Delta Table
# ----------------------------
def create_delta_table(df, schema, table, loc):

    df.createOrReplaceTempView("test")

    if schema.lower() in loc.lower() and table.lower() in loc.lower():

        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        dbutils.fs.rm(loc, True)

        spark.sql(f"""
        CREATE TABLE {schema}.{table}
        USING DELTA
        PARTITIONED BY(incr_ingest_timestamp)
        LOCATION '{loc}'
        TBLPROPERTIES (
            delta.autoOptimize.optimizeWrite = true,
            delta.autoOptimize.autoCompact = true
        )
        AS SELECT * FROM test
        """)
    else:
        raise Exception("Invalid table location")

# ----------------------------
# CORE INGESTION LOGIC
# ----------------------------
def run_ingestion(env, config_path):

    try:
        logger.info(f"Starting ingestion for config: {config_path}")

        conf = load_config(config_path)
        file_conf = conf["filebasedconf"]

        file_type = file_conf["type"]
        delimiter = file_conf["delimiter"]
        include_header = file_conf["include_header"]
        bucket = file_conf["bucket_name"]
        prefix = file_conf["file_path"]

        source_schema = file_conf["schema"]
        target_schema = file_conf.get("target_schema")

        table_schema = file_conf["table_schema"]
        table_name = file_conf["table_name"]
        loc = file_conf["location"]

        cols = file_conf.get("columnsWithDollar", "").split(",") if "columnsWithDollar" in file_conf else []

        pick_all_files = file_conf.get("pick_all_files", "N")
        append_ind = file_conf.get("append_ind", False)
        file_del_ind = file_conf.get("filedel_ind", "Y")

        # ----------------------------
        # S3 LIST FILES
        # ----------------------------
        s3 = boto3.client("s3")

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [f"s3://{bucket}/{obj['Key']}" for obj in response.get("Contents", [])]

        if not files:
            raise Exception("No files found")

        logger.info(f"Files found: {files}")

        file_path = f"s3://{bucket}/{prefix}/"
        delta_exists = spark.catalog.tableExists(f"{table_schema}.{table_name}")

        # ----------------------------
        # PROCESS FILES
        # ----------------------------
        for file in files:

            file_name = file.split("/")[-1]
            logger.info(f"Processing file: {file_name}")

            source_fields, target_fields = get_field_struct(source_schema, target_schema)

            if file_type == "csv":
                schema = StructType(source_fields)
                df = spark.read.option("header", include_header == "Y") \
                    .option("delimiter", delimiter) \
                    .schema(schema) \
                    .csv(files)
            else:
                df = spark.read.text(file_path if pick_all_files == "Y" else file)

            if df.rdd.isEmpty():
                raise Exception(f"{file_name} is empty")

            df = transform_with_mapping(df, source_fields, target_fields)

            df = df.withColumn("file_name", lit(file_name)) \
                   .withColumn("incr_ingest_timestamp", current_timestamp())

            # Cleaning
            for c in cols:
                df = df.withColumn(
                    c,
                    when(trim(col(c)) == "-", None)
                    .otherwise(regexp_replace(trim(col(c)), "(#?N/A|#VALUE!)|[$, ]", ""))
                )

            # Write Logic
            if delta_exists and append_ind:

                existing_files = spark.table(f"{table_schema}.{table_name}") \
                    .select("file_name").distinct().rdd.map(lambda r: r[0]).collect()

                if file_name in existing_files:
                    logger.info(f"Skipping already processed file: {file_name}")
                    continue

                df = apply_original_schema(df, StructType(source_fields))

                df.createOrReplaceTempView("INC_DATA")

                spark.sql(f"""
                INSERT INTO {table_schema}.{table_name}
                SELECT * FROM INC_DATA
                """)
            else:
                create_delta_table(df, table_schema, table_name, loc)

        # Delete Files
        if file_del_ind == "Y":
            for file in files:
                dbutils.fs.rm(file, True)

        logger.info("Ingestion completed successfully")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# ----------------------------
# API ENDPOINT
# ----------------------------
@app.post("/run-ingestion")
def trigger_ingestion(request: IngestionRequest, background_tasks: BackgroundTasks):

    try:
        background_tasks.add_task(run_ingestion, request.env, request.config_path)

        return {
            "status": "started",
            "message": "Ingestion job triggered"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ----------------------------
# HEALTH CHECK
# ----------------------------
@app.get("/health")
def health():
    return {"status": "running"}