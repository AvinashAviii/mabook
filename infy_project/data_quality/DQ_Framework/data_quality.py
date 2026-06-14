import sys
from pyspark.sql import SparkSession
from dq_models import DataQualityRequest
from dq_utils import ConfigParser

# Import the feature modules (which we define below)
import data_quality_profiler
import data_quality_suggestion
import data_quality_verifier
import data_quality_suggestion_load
import data_quality_profile_load
import data_quality_anamoly


class DataQuality:
    ARG_VAL_DATA_QUALITY_PROFILE = "profile"
    ARG_VAL_DATA_QUALITY_SUGGESTION = "suggestion"
    ARG_VAL_DATA_QUALITY_VALIDATION = "validation"
    ARG_VAL_DATA_QUALITY_METRICS = "metrics"
    ARG_VAL_DATA_QUALITY_SUGGEST_LOAD = "dqvalidateload"
    ARG_VAL_DATA_QUALITY_PROFILE_LOAD = "dqprofileload"
    ARG_VAL_DATA_QUALITY_REJECT_CAPTURE = "dqreject"
    ARG_VAL_DATA_QUALITY_OPTIMIZATION = "optimize"

    rulestable = "dq_metrics_trn.data_quality_rules"
    rulesmanualtable = "dq_metrics_trn.data_quality_manual_rules"
    profilerulestable = "dq_metrics_trn.data_quality_profile_rules"
    profiletable = "dq_metrics_trn.data_quality_profile_results"
    checkresultstable = "dq_metrics_trn.data_quality_check_results"
    metricsresultstable = "dq_metrics_trn.data_quality_metrics_results"
    dqmetricsext = "dq_metrics_trn_ext"

    @staticmethod
    def main(args: list):
        if not args:
            raise Exception("Config argument missing")

        config = ConfigParser.parse_config(args[0])
        module = config.get("module")
        env = config.get("env")
        db_name = config.get("dbName")

        if module == DataQuality.ARG_VAL_DATA_QUALITY_OPTIMIZATION:
            if not module or not env:
                raise Exception(
                    "Env and Module are required for DQ table optimization")
        else:
            if not module or not env or not db_name:
                raise Exception(
                    "DB Name, Env and Module are required for checking DQ")

        col_name_str = config.get("colName")
        col_list = [c.strip() for c in col_name_str.split(",")
                    ] if col_name_str else None

        dqr = DataQualityRequest(
            env=env, module=module, dbName=db_name,
            tableName=config.get("tableName"), filterCondition=config.get("whereClause"),
            colNames=col_list, path=config.get("path"), data=None,
            rejectCaptureFlag=config.get("rejectCaptureFlag", "N")
        )
        DataQuality.process_dq(dqr)

    @staticmethod
    def process_dq(dqr: DataQualityRequest):
        spark = SparkSession.builder.appName(
            f"DataQuality - {dqr.module}").getOrCreate()

        if dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_PROFILE:
            data_quality_profiler.DataQualityProfiler.profile_table(dqr, spark)
        elif dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_SUGGESTION:
            data_quality_suggestion.DataQualitySuggestion.dq_suggest(
                dqr, spark)
        elif dqr.module in [DataQuality.ARG_VAL_DATA_QUALITY_VALIDATION, DataQuality.ARG_VAL_DATA_QUALITY_METRICS]:
            data_quality_verifier.DataQualityVerifier.dq_verify(dqr, spark)
        elif dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_SUGGEST_LOAD:
            data_quality_suggestion_load.DataQualitySuggestionLoad.dq_load(
                dqr, spark)
        elif dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_PROFILE_LOAD:
            data_quality_profile_load.DataQualityProfileLoad.dq_load(
                dqr, spark)
        elif dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_REJECT_CAPTURE:
            data_quality_anamoly.DataQualityAnamoly.capture_data_anamolies(dqr, [
            ], spark)
        elif dqr.module == DataQuality.ARG_VAL_DATA_QUALITY_OPTIMIZATION:
            DataQuality.delta_table_cleanup(
                "dq_metrics_trn", "data_quality_check_results", spark)

    @staticmethod
    def delta_table_cleanup(db: str, tbl: str, spark: SparkSession, retain_duration="168"):
        spark.sql(f"OPTIMIZE {db}.{tbl}")
        spark.sql(f"VACUUM {db}.{tbl} RETAIN {retain_duration} HOURS")


if __name__ == "__main__":
    DataQuality.main(sys.argv[1:])
