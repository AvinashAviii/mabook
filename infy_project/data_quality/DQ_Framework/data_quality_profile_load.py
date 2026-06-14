from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_models import DataQualityRequest
from dq_utils import AthenaTableRefresh
from data_quality import DataQuality


class DataQualityProfileLoad:
    RULE_SCHEMA = ["db_name", "table_name", "column_name", "is_active"]

    @staticmethod
    def dq_load(dqr: DataQualityRequest, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")

        df = spark.read.option("header", "true").option(
            "inferSchema", "true").option("sep", "|").csv(dqr.path).persist()
        db_names = [row[0]
                    for row in df.select("db_name").distinct().collect()]

        if len(db_names) != 1 or dqr.dbName not in db_names:
            df.unpersist()
            raise Exception(
                "DB Name in the input CSV and the parameter must match.")

        invalid_cols = [
            c for c in df.columns if c not in DataQualityProfileLoad.RULE_SCHEMA]
        if invalid_cols:
            df.unpersist()
            raise Exception(
                "CSV structure does not match template: db_name,table_name,column_name,is_active")

        spark.sql(
            f"DELETE FROM {DataQuality.profilerulestable} WHERE db_name = '{dqr.dbName}'")
        df.withColumn("audit_create_date", F.current_timestamp()).write.mode(
            "append").insertInto(DataQuality.profilerulestable)

        db, tbl = DataQuality.profilerulestable.split(".")
        DataQuality.delta_table_cleanup(db, tbl, spark)
        AthenaTableRefresh.refresh_athena_table(db, tbl)
        df.unpersist()
