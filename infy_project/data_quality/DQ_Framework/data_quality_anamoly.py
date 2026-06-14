from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_models import DataQualityRequest
from dq_utils import AthenaTableRefresh, AthenaTableCreate
from data_quality import DataQuality


class DataQualityAnamoly:
    trn_db = "dq_metrics_trn"

    @staticmethod
    def capture_data_anamolies(dqr: DataQualityRequest, failed_ds: list, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")

        dq_configs = DataQualityAnamoly._select_dq_config(dqr, "verify", spark)
        spark.sql(f"USE {DataQualityAnamoly.trn_db}")
        tables = [t.name.lower()
                  for t in spark.catalog.listTables(DataQualityAnamoly.trn_db)]
        tbl_nm = f"{dqr.dbName}_rejects"

        if dq_configs:
            for row in dq_configs:
                db_nm = row['db_name']
                table_nm = row['table_name']
                constraint_nm = row['constraint_name']

                # Check if this rule failed
                is_failed = not failed_ds or any(
                    s.db_name == db_nm and s.table_name == table_nm and s.constraint_name == constraint_nm
                    for s in failed_ds
                )

                if is_failed:
                    bad_df = spark.sql(row['reject_sql'])
                    cols = bad_df.columns
                    final_df = bad_df.withColumn("query_result", F.to_json(F.struct(*[F.col(c) for c in cols]))) \
                                     .withColumn("db_name", F.lit(db_nm)) \
                                     .withColumn("table_name", F.lit(table_nm)) \
                                     .withColumn("constraint_name", F.lit(constraint_nm)) \
                                     .withColumn("audit_create_date", F.current_timestamp()) \
                                     .select("query_result", "db_name", "table_name", "constraint_name", "audit_create_date")
                    final_df.cache()

                    if not final_df.isEmpty():
                        target = f"s3://tbdp-trn-{dqr.env}/{DataQualityAnamoly.trn_db}/{tbl_nm}"
                        final_df.write.format("delta").mode(
                            "append").save(target)
                    final_df.unpersist()

            if tbl_nm.lower() not in tables:
                spark.sql(f"""
                    CREATE TABLE {DataQualityAnamoly.trn_db}.{tbl_nm.lower()} 
                    (query_result STRING, db_name STRING, table_name STRING, constraint_name STRING, audit_create_date TIMESTAMP)
                    USING DELTA LOCATION 's3://tbdp-trn-{dqr.env}/{DataQualityAnamoly.trn_db}/{tbl_nm.lower()}'
                """)
                athena_sql, _ = AthenaTableCreate.create_sql(DataQualityAnamoly.trn_db, tbl_nm.lower(
                ), f"tbdp-trn-{dqr.env}", f"{tbl_nm.lower()}_athena")
                spark.sql(athena_sql)

            DataQuality.delta_table_cleanup(
                DataQualityAnamoly.trn_db, tbl_nm, spark)
            AthenaTableRefresh.refresh_athena_table(
                DataQualityAnamoly.trn_db, tbl_nm)

    @staticmethod
    def _select_dq_config(dqr, rule_type: str, spark: SparkSession):
        df = spark.table(DataQuality.rulesmanualtable).filter(
            f"is_active = 'Y' and reject_capture = 'Y' and db_name = '{dqr.dbName}' and rule_type = '{rule_type}' and reject_sql is not null"
        )
        if dqr.tableName:
            df = df.filter(F.col("table_name").isin(
                [t.strip() for t in dqr.tableName.split(",")]))
        if dqr.colNames:
            df = df.filter(F.col("column_name").isin(dqr.colNames))
        return df.select("db_name", "table_name", "constraint_name", "reject_sql").distinct().collect()
