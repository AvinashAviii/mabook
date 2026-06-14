import os
import uuid
import logging
import pandas as pd
from typing import Dict, Any, Optional, Tuple
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StringType, IntegerType, DoubleType,
    DateType, TimestampType, BooleanType, LongType
)

from core.spark_manager import spark_manager
from config import settings

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data ingestion from various sources and profiling"""

    def __init__(self):
        self.spark = spark_manager.spark
        self._datasets: Dict[str, SparkDataFrame] = {}
        self._metadata: Dict[str, Dict] = {}

    def load_csv(
        self,
        file_path: str,
        dataset_id: Optional[str] = None,
        infer_schema: bool = True,
        header: bool = True
    ) -> Tuple[str, Dict[str, Any]]:
        """Load CSV file into PySpark DataFrame"""

        dataset_id = dataset_id or str(uuid.uuid4())[:8]

        logger.info(f"Loading CSV: {file_path} as dataset_id={dataset_id}")

        df = (
            self.spark.read
            .option("header", str(header).lower())
            .option("inferSchema", str(infer_schema).lower())
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(file_path)
        )

        # Cache for repeated operations
        df.cache()
        df.count()  # materialize cache

        self._datasets[dataset_id] = df

        # Generate metadata and profile
        metadata = self._profile_dataset(dataset_id, df, file_path)
        self._metadata[dataset_id] = metadata

        logger.info(
            f"Dataset '{dataset_id}' loaded: "
            f"{metadata['row_count']} rows × {metadata['column_count']} cols"
        )
        return dataset_id, metadata

    def load_from_pandas(
        self,
        pdf: pd.DataFrame,
        dataset_id: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Load from Pandas DataFrame"""
        dataset_id = dataset_id or str(uuid.uuid4())[:8]
        df = self.spark.createDataFrame(pdf)
        df.cache()
        df.count()

        self._datasets[dataset_id] = df
        metadata = self._profile_dataset(dataset_id, df, "pandas_upload")
        self._metadata[dataset_id] = metadata

        return dataset_id, metadata

    def get_dataset(self, dataset_id: str) -> SparkDataFrame:
        """Retrieve a loaded dataset"""
        if dataset_id not in self._datasets:
            raise KeyError(
                f"Dataset '{dataset_id}' not found. Available: {list(self._datasets.keys())}")
        return self._datasets[dataset_id]

    def get_metadata(self, dataset_id: str) -> Dict[str, Any]:
        """Retrieve dataset metadata"""
        if dataset_id not in self._metadata:
            raise KeyError(f"Metadata for '{dataset_id}' not found.")
        return self._metadata[dataset_id]

    def get_sample(self, dataset_id: str, n: int = 10) -> list:
        """Get sample rows as list of dicts"""
        df = self.get_dataset(dataset_id)
        return [row.asDict() for row in df.limit(n).collect()]

    def _profile_dataset(
        self,
        dataset_id: str,
        df: SparkDataFrame,
        source: str
    ) -> Dict[str, Any]:
        """Generate comprehensive data profile"""

        row_count = df.count()
        columns = df.columns
        dtypes = {field.name: str(field.dataType)
                  for field in df.schema.fields}

        # Per-column profiling
        column_profiles = {}
        for col_name in columns:
            col_type = str(df.schema[col_name].dataType)
            nullable = df.schema[col_name].nullable

            stats = df.select(
                F.count(F.col(col_name)).alias("non_null_count"),
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(
                    0)).alias("null_count"),
                F.countDistinct(F.col(col_name)).alias("distinct_count"),
            ).collect()[0]

            profile = {
                "data_type": col_type,
                "nullable": nullable,
                "non_null_count": stats["non_null_count"],
                "null_count": stats["null_count"],
                "null_percentage": round(
                    (stats["null_count"] / row_count *
                     100) if row_count > 0 else 0, 2
                ),
                "distinct_count": stats["distinct_count"],
                "uniqueness_ratio": round(
                    (stats["distinct_count"] / row_count *
                     100) if row_count > 0 else 0, 2
                ),
            }

            # Numeric stats
            if col_type in ("IntegerType()", "LongType()", "DoubleType()", "FloatType()"):
                num_stats = df.select(
                    F.min(F.col(col_name)).alias("min_val"),
                    F.max(F.col(col_name)).alias("max_val"),
                    F.avg(F.col(col_name)).alias("mean_val"),
                    F.stddev(F.col(col_name)).alias("stddev_val"),
                ).collect()[0]
                profile.update({
                    "min_value": num_stats["min_val"],
                    "max_value": num_stats["max_val"],
                    "mean_value": round(num_stats["mean_val"], 2) if num_stats["mean_val"] else None,
                    "stddev": round(num_stats["stddev_val"], 2) if num_stats["stddev_val"] else None,
                })

            # String stats
            if col_type == "StringType()":
                str_stats = df.select(
                    F.min(F.length(F.col(col_name))).alias("min_length"),
                    F.max(F.length(F.col(col_name))).alias("max_length"),
                    F.avg(F.length(F.col(col_name))).alias("avg_length"),
                ).collect()[0]
                profile.update({
                    "min_length": str_stats["min_length"],
                    "max_length": str_stats["max_length"],
                    "avg_length": round(str_stats["avg_length"], 2) if str_stats["avg_length"] else None,
                })

                # Top values
                top_vals = (
                    df.filter(F.col(col_name).isNotNull())
                    .groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(10)
                    .collect()
                )
                profile["top_values"] = {
                    row[col_name]: row["count"] for row in top_vals
                }

            column_profiles[col_name] = profile

        # Sample data
        sample_rows = [row.asDict() for row in df.limit(5).collect()]
        # Convert non-serializable types
        for row in sample_rows:
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()

        return {
            "dataset_id": dataset_id,
            "source": source,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "dtypes": dtypes,
            "column_profiles": column_profiles,
            "sample_data": sample_rows,
        }

    def list_datasets(self) -> list:
        """List all loaded datasets"""
        return [
            {
                "dataset_id": did,
                "row_count": meta.get("row_count"),
                "column_count": meta.get("column_count"),
                "source": meta.get("source"),
            }
            for did, meta in self._metadata.items()
        ]


# Global singleton
data_loader = DataLoader()
