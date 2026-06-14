import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from dq_models import DataQualityRequest
from dq_utils import DateFunctions, AthenaTableRefresh, SNSUtil, AWSUtil
from data_quality import DataQuality
from data_quality_anamoly import DataQualityAnamoly


class DataQualityVerifier:
    logger = logging.getLogger(__name__)
    failed_ds = []

    @staticmethod
    def dq_verify(dqr: DataQualityRequest, spark: SparkSession):
        if not dqr.dbName:
            raise Exception("DB Name parameter is required")

        rule_type = "metrics" if dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_METRICS else "verify"
        configs = DataQualityVerifier._get_configs(dqr, rule_type, spark)

        tbls = list({(r['table_name'], r['sql']) for r in configs})

        for t_name, custom_sql in tbls:
            rules = [r for r in configs if r['table_name'] == t_name]

            if dqr.data is None:
                if not custom_sql:
                    df = spark.table(f"{dqr.dbName}.{t_name}")
                else:
                    df = spark.sql(custom_sql)
                if dqr.filterCondition:
                    df = df.filter(dqr.filterCondition)
            else:
                df = dqr.data

            cols = list(
                {c.strip() for r in rules if r['column_name'] for c in r['column_name'].split(",")})
            if cols:
                df = df.select(*[F.col(c) for c in cols])

            DataQualityVerifier._process_checks(df, dqr, rules, t_name, spark)

        db, tbl = DataQuality.checkresultstable.split(
            ".") if dqr.module != "metrics" else DataQuality.metricsresultstable.split(".")
        AthenaTableRefresh.refresh_athena_table(db, tbl)
        AthenaTableRefresh.repair_athena_table(DataQuality.dqmetricsext, tbl)

        DataQualityVerifier._notify(dqr, DataQualityVerifier.failed_ds, spark)
        DataQualityAnamoly.capture_data_anamolies(
            dqr, DataQualityVerifier.failed_ds, spark)

    @staticmethod
    def _process_checks(df, dqr, rules, tbl_nm, spark):
        date_str = DateFunctions.time_now("%Y-%m-%d %H:%M:%S")
        date_month = int(DateFunctions.time_now("%Y%m"))

        if dqr.module != "metrics":
            # REPLACING SCALA TOOLBOX: Dynamic PyDeequ construction
            check_obj = Check(spark, CheckLevel.Error, f"{tbl_nm}_check")

            for row in rules:
                level = CheckLevel.Error if row['check_level'].lower(
                ) == "error" else CheckLevel.Warning

                # Python Evaluation of rule strings from database (e.g., '.isComplete("email")')
                # Warning: Requires the DB constraint_rule to exactly match PyDeequ syntax
                rule_str = row['constraint_rule'].strip()
                try:
                    # Dynamically applies the rule: check_obj = check_obj.isComplete("email")
                    check_obj = eval(f"check_obj{rule_str}")
                except Exception as e:
                    DataQualityVerifier.logger.error(
                        f"Rule evaluation failed for {rule_str}: {e}")

            result = VerificationSuite(spark).onData(
                df).addCheck(check_obj).run()
            res_df = VerificationResult.checkResultsAsDataFrame(spark, result)

            final_df = res_df.withColumn("db_name", F.lit(dqr.dbName)) \
                             .withColumn("table_name", F.lit(tbl_nm)) \
                             .withColumn("audit_create_date", F.lit(date_str)) \
                             .withColumn("create_month_prtn", F.lit(date_month))

            final_df.write.mode("append").insertInto(
                DataQuality.checkresultstable)

            # Store failures for SNS notifications
            failed_rows = final_df.filter(
                "constraint_status != 'Success'").collect()
            DataQualityVerifier.failed_ds.extend(failed_rows)

    @staticmethod
    def _notify(dqr, failed_list, spark):
        if not failed_list:
            return
        rules_tbl = spark.table(DataQuality.rulesmanualtable)
        notifications = {}

        for row in failed_list:
            # We assume row attributes map to pydeequ check results df
            config = rules_tbl.filter((F.col("db_name") == row.db_name) & (F.col(
                "table_name") == row.table_name)).select("notify_capture", "notify_topic").first()
            if config and config.notify_capture == "Y":
                topic = config.notify_topic
                notifications[topic] = notifications.get(
                    topic, "") + f"{row.db_name}-{row.table_name}\n"

        for topic, msg in notifications.items():
            SNSUtil.publish_notification(
                dqr.env, topic, f"DQ Rules failure for {dqr.dbName}", msg, "sns_session", AWSUtil.get_iam_spect_role(dqr.env))

    @staticmethod
    def _get_configs(dqr, rule_type, spark):
        df = spark.table(DataQuality.rulesmanualtable).filter(
            f"is_active = 'Y' and db_name = '{dqr.dbName}' and rule_type = '{rule_type}'")
        if dqr.tableName:
            df = df.filter(F.col("table_name").isin(
                [t.strip() for t in dqr.tableName.split(",")]))

        res = df.select(
            F.lower("db_name").alias("db_name"), F.lower(
                "table_name").alias("table_name"),
            F.lower("column_name").alias(
                "column_name"), "sql", "constraint_name",
            "constraint_rule", "check_level", "check_message"
        ).distinct().collect()

        if not res:
            raise Exception("Configuration missing")
        return [r.asDict() for r in res]
