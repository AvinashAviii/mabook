import logging
from pyspark.sql import SparkSession
from config import settings

logger = logging.getLogger(__name__)


class SparkManager:
    """Singleton PySpark session manager"""
    _instance = None
    _spark: SparkSession = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create_session(self) -> SparkSession:
        if self._spark is None or self._spark._sc._jsc is None:
            logger.info("Creating new SparkSession...")
            self._spark = (
                SparkSession.builder
                .appName(settings.SPARK_APP_NAME)
                .master(settings.SPARK_MASTER)
                .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY)
                .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
            logger.info(f"SparkSession created: {self._spark.version}")
        return self._spark

    def stop(self):
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("SparkSession stopped")

    @property
    def spark(self) -> SparkSession:
        return self.get_or_create_session()


# Global singleton
spark_manager = SparkManager()