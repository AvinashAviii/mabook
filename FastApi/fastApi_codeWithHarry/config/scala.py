# bcp_file_based_ingestion.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyhocon import ConfigFactory
import boto3
import logging

# ----------------------------
# Logger
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BCP-INGESTION")

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder.appName("BCPFileBasedIngestion").getOrCreate()

# ----------------------------
# Load Config
# ----------------------------
def load_config(path):
    txt = "\n".join(spark.read.text(path).rdd.map(lambda r: r[0]).collect())
    return ConfigFactory.parse_string(txt)

# ----------------------------
# Get Schema (Source + Target)
# ----------------------------
def get_field_struct(source_schema, target_schema=None):

    def parse_schema(schema_str):
        fields = []
        for field in schema_str.split(","):
            name, dtype = field.strip().split(" ")

            dtype = dtype.lower()
            if dtype == "string":
                spark_type = StringType()
            elif dtype == "double":
                spark_type = DoubleType()
            elif dtype == "int":
                spark_type = IntegerType()
            elif dtype == "date":
                spark_type = DateType()
            elif dtype == "number":
                spark_type = LongType()
            elif dtype == "timestamp":
                spark_type = TimestampType()
            else:
                raise Exception(f"Unsupported datatype: {dtype}")

            fields.append(StructField(name, spark_type, True))

        return fields

    source_fields = parse_schema(source_schema)
    target_fields = parse_schema(target_schema) if target_schema else None

    return source_fields, target_fields

# ----------------------------
# Apply Original Schema
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
        source_cols = [f.name for f in source_fields]
        target_cols = [f.name for f in target_fields]

        mapping = dict(zip(source_cols, target_cols))

        logger.info(f"Column Mapping: {mapping}")

        return df.select([
            col(c).alias(mapping.get(c, c)) for c in source_cols
        ])
    else:
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
# MAIN FUNCTION
# ----------------------------
def main(args):

    env = args[0]
    conf = load_config(args[2])

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

    data_masking = file_conf.get("dataMasking", "N")
    masking_rules = ConfigFactory.parse_string(file_conf.get("maskingRulesStr", "{}"))

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

    logger.info(f"Files: {files}")

    file_path = f"s3://{bucket}/{prefix}/"

    delta_exists = spark.catalog.tableExists(f"{table_schema}.{table_name}")

    # ----------------------------
    # PROCESS FILES
    # ----------------------------
    for file in files:

        file_name = file.split("/")[-1]
        logger.info(f"Processing: {file_name}")

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
            raise Exception("File is empty")

        df.printSchema()
        df.show(3)

        # ----------------------------
        # Transform
        # ----------------------------
        df = transform_with_mapping(df, source_fields, target_fields)

        df = df.withColumn("file_name", lit(file_name)) \
               .withColumn("incr_ingest_timestamp", current_timestamp())

        # ----------------------------
        # Data Cleaning
        # ----------------------------
        for c in cols:
            df = df.withColumn(
                c,
                when(trim(col(c)) == "-", None)
                .otherwise(regexp_replace(trim(col(c)), "(#?N/A|#VALUE!)|[$, ]", ""))
            )

        # ----------------------------
        # Write Logic
        # ----------------------------
        if delta_exists and append_ind:

            existing_files = spark.table(f"{table_schema}.{table_name}") \
                .select("file_name").distinct().rdd.map(lambda r: r[0]).collect()

            if file_name in existing_files:
                logger.info("File already processed")
                continue

            df = apply_original_schema(df, StructType(source_fields))

            df.createOrReplaceTempView("INC_DATA")

            spark.sql(f"""
            INSERT INTO {table_schema}.{table_name}
            SELECT * FROM INC_DATA
            """)

        else:
            create_delta_table(df, table_schema, table_name, loc)

    # ----------------------------
    # Delete Files
    # ----------------------------
    if file_del_ind == "Y":
        for file in files:
            dbutils.fs.rm(file, True)

    logger.info("SUCCESS")

# ----------------------------
# ENTRY POINT
# ----------------------------
if __name__ == "__main__":
    args = dbutils.widgets.get("args")
    main(args)