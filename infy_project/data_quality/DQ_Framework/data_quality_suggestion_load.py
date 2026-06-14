from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_models import DataQualityRequest
from dq_utils import AthenaTableRefresh
from data_quality import DataQuality


class DataQualitySuggestionLoad:
    RULE_SCHEMA = ["db_name", "table_name", "column_name", "sql", "constraint_name", "constraint_rule",
                   "constraint_desc", "is_active", "reject_capture", "reject_sql", "rule_type", "check_level", "check_message"]
    RULE_NEW_SCHEMA = RULE_SCHEMA + ["notify_capture", "notify_topic"]

    @staticmethod
    def dq_load(dqr: DataQualityRequest, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")

        df = spark.read.option("header", "true").option(
            "inferSchema", "true").option("sep", "|").csv(dqr.path).persist()

        if dqr.dbName not in [r[0] for r in df.select("db_name").distinct().collect()]:
            raise Exception("DB Name mismatch.")

        if df.count() != df.select("db_name", "table_name", "constraint_name").distinct().count():
            raise Exception(
                "Combination of DB, Table, and Constraint Name must be unique.")

        col_len = len(df.columns)
        if col_len == 13 and any(c not in DataQualitySuggestionLoad.RULE_SCHEMA for c in df.columns):
            raise Exception("Schema mismatch for 13 columns.")
        if col_len == 15 and any(c not in DataQualitySuggestionLoad.RULE_NEW_SCHEMA for c in df.columns):
            raise Exception("Schema mismatch for 15 columns.")

        if dqr.rejectCaptureFlag != "N" and not df.filter("reject_capture = 'N' OR reject_sql IS NULL").isEmpty():
            raise Exception(
                "reject_capture must be enabled and reject_sql provided.")

        db, tbl = DataQuality.rulesmanualtable.split(".")
        spark.sql(
            f"DELETE FROM {DataQuality.rulesmanualtable} WHERE db_name = '{dqr.dbName}'")

        target = f"s3://tbdp-trn-{dqr.env}/{db}/{tbl}"
        df.withColumn("audit_create_date", F.current_timestamp()).write.format(
            "delta").option("mergeSchema", "true").mode("append").save(target)

        DataQuality.delta_table_cleanup(db, tbl, spark)
        AthenaTableRefresh.refresh_athena_table(db, tbl)
