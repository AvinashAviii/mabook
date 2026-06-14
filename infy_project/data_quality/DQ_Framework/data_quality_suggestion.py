from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_models import DataQualityRequest
from dq_utils import DateFunctions, AthenaTableRefresh
from data_quality import DataQuality
from pydeequ.suggestions import ConstraintSuggestionRunner, Rules


class DataQualitySuggestion:
    @staticmethod
    def dq_suggest(dqr: DataQualityRequest, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")

        tables = DataQualitySuggestion._get_tables(dqr, spark)
        for t in tables:
            df = spark.table(f"{dqr.dbName}.{t}")
            if dqr.colNames:
                df = df.select(*[F.col(c) for c in dqr.colNames])
            if dqr.filterCondition:
                df = df.filter(dqr.filterCondition)
            DataQualitySuggestion._update_suggestions(df, dqr, t, spark)

        db, tbl = DataQuality.rulestable.split(".")
        DataQuality.delta_table_cleanup(db, tbl, spark)
        AthenaTableRefresh.refresh_athena_table(db, tbl)

    @staticmethod
    def _update_suggestions(df, dqr, tbl, spark):
        result = ConstraintSuggestionRunner(spark).onData(
            df).addConstraintRules(Rules.DEFAULT).run()
        date_str = DateFunctions.time_now("%Y-%m-%d %H:%M:%S")

        rows = []
        for col, suggestions in result.constraintSuggestions.items():
            for s in suggestions:
                rows.append({
                    "db_name": dqr.dbName, "table_name": tbl, "column_name": col,
                    "constraint_rule": s.constraint.code, "constraint_desc": s.constraint.description,
                    "is_active": "N", "reject_capture": "N", "rule_type": "verify",
                    "check_level": "Error", "check_message": s.constraint.code, "audit_create_date": date_str
                })

        if not rows:
            return

        spark.sql(
            f"DELETE FROM {DataQuality.rulestable} WHERE db_name='{dqr.dbName}' AND table_name='{tbl}' AND rule_type='verify' AND is_active='N'")
        new_df = spark.createDataFrame(rows)
        new_df.createOrReplaceTempView("s_temp")
        spark.table(DataQuality.rulestable).createOrReplaceTempView("s_vw")

        spark.sql("""
            SELECT t.* FROM s_temp t LEFT JOIN s_vw r 
            ON r.db_name=t.db_name AND r.table_name=t.table_name AND r.column_name=t.column_name AND r.constraint_rule=t.constraint_rule 
            WHERE r.is_active IS NULL
        """).write.mode("append").insertInto(DataQuality.rulestable)

    @staticmethod
    def _get_tables(dqr, spark):
        if not dqr.tableName:
            return [t.name for t in spark.catalog.listTables(dqr.dbName) if "athena" not in t.name.lower()]
        return [t.strip() for t in dqr.tableName.split(",")]
