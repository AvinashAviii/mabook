import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "DataQuality Framework"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True

    # Gemini AI
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = "gemini-1.5-flash"
    GEMINI_TEMPERATURE: float = 0.2
    GEMINI_MAX_TOKENS: int = 8192

    # PySpark
    SPARK_APP_NAME: str = "DQ_Framework"
    SPARK_MASTER: str = "local[*]"
    SPARK_DRIVER_MEMORY: str = "4g"
    SPARK_EXECUTOR_MEMORY: str = "4g"

    # Alerting
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    ALERT_RECIPIENTS: list = ["dq-alerts@company.com"]

    # AWS SNS (optional)
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    SNS_TOPIC_ARN: str = os.getenv("SNS_TOPIC_ARN", "")

    # Storage
    UPLOAD_DIR: str = "./uploads"
    REPORT_DIR: str = "./reports"

    # Thresholds
    DQ_PASS_THRESHOLD: float = 95.0  # percentage
    ANOMALY_SAMPLE_SIZE: int = 100

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
