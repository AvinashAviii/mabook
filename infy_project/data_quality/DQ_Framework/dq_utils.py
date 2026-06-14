from datetime import datetime


class DateFunctions:
    @staticmethod
    def time_now(fmt: str) -> str:
        # Simplifies Scala's dateFormats into standard Python strftime
        return datetime.now().strftime(fmt)


class AthenaTableRefresh:
    @staticmethod
    def refresh_athena_table(db: str, tbl: str):
        print(f"Mock: Refreshing Athena table {db}.{tbl}")

    @staticmethod
    def repair_athena_table(db: str, tbl: str):
        print(f"Mock: Repairing Athena table {db}.{tbl}")


class AthenaTableCreate:
    @staticmethod
    def create_sql(db, tbl, location, athena_tbl):
        return f"CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{athena_tbl} ...", ""


class SNSUtil:
    @staticmethod
    def publish_notification(env, topic, subject, message, session, role):
        print(f"Mock: Sending SNS to {topic}: {subject} -> {message}")


class AWSUtil:
    @staticmethod
    def get_iam_spect_role(env: str) -> str:
        return f"arn:aws:iam::mock:role/{env}-role"


class ConfigParser:
    @staticmethod
    def parse_config(arg: str) -> dict:
        # Mock parser - replace with your actual JSON/YAML/CLI config loader
        return {"module": "validation", "env": "dev", "dbName": "test_db", "rejectCaptureFlag": "Y"}
