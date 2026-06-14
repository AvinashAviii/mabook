from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_models import DataQualityRequest
from dq_utils import DateFunctions, AthenaTableRefresh
from data_quality import DataQuality
from pydeequ.profiles import ColumnProfilerRunner


class DataQualityProfiler:
    @staticmethod
    def profile_table(dqr: DataQualityRequest, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")
        if not dqr.tableName and dqr.colNames:
            raise Exception("Table Name is required when colname is specified")

        dq_configs = DataQualityProfiler._select_dq_config(dqr, spark)
        tables = {row["table_name"] for row in dq_configs}

        for t in tables:
            print(f"Processing {t}")
            df = spark.table(f"{dqr.dbName}.{t}")

            cols = [r["column_name"]
                    for r in dq_configs if r["table_name"] == t and r["column_name"]]
            if cols:
                df = df.select(*[F.col(c) for c in set(cols)])
            if dqr.filterCondition:
                df = df.filter(dqr.filterCondition)

            DataQualityProfiler._update_stats(df, dqr, t, spark)

        db, tbl = DataQuality.profiletable.split(".")
        DataQuality.delta_table_cleanup(db, tbl, spark)
        AthenaTableRefresh.refresh_athena_table(db, tbl)
        AthenaTableRefresh.repair_athena_table(DataQuality.dqmetricsext, tbl)

    @staticmethod
    def _update_stats(df, dqr, tbl, spark):
        result = ColumnProfilerRunner(spark).onData(df).run()
        date_time = DateFunctions.time_now("%Y-%m-%d %H:%M:%S")
        date_month = int(DateFunctions.time_now("%Y%m"))

        rows = []
        for col_name, profile in result.profiles.items():
            rows.append({
                "db_name": dqr.dbName, "table_name": tbl, "column_name": col_name,
                "completeness": round(profile.completeness, 2), "data_type": str(profile.dataType),
                "approx_num_distinct_values": int(profile.approximateNumDistinctValues),
                "audit_create_date": date_time, "create_month_prtn": date_month
            })

        spark.createDataFrame(rows).coalesce(1).write.mode(
            "append").insertInto(DataQuality.profiletable)

    @staticmethod
    def _select_dq_config(dqr, spark):
        if dqr.colNames:
            return [{"db_name": dqr.dbName, "table_name": dqr.tableName, "column_name": c} for c in dqr.colNames]

        df = spark.table(DataQuality.profilerulestable).filter(
            f"is_active = 'Y' and db_name = '{dqr.dbName}'")
        if dqr.tableName:
            df = df.filter(f"table_name = '{dqr.tableName}'")

        configs = [r.asDict() for r in df.select(
            "db_name", "table_name", "column_name").distinct().collect()]
        if not configs:
            if not dqr.tableName:
                tables = spark.catalog.listTables(dqr.dbName)
                return [{"db_name": dqr.dbName, "table_name": t.name, "column_name": None} for t in tables if "athena" not in t.name.lower()]
            else:
                return [{"db_name": dqr.dbName, "table_name": t.strip(), "column_name": None} for t in dqr.tableName.split(",")]
        return configs
