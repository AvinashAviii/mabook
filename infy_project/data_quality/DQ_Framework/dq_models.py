from dataclasses import dataclass
from typing import List, Optional, Any
from pyspark.sql import DataFrame


@dataclass
class DataQualityRequest:
    env: str
    module: str
    dbName: str
    tableName: Optional[str]
    filterCondition: Optional[str]
    colNames: Optional[List[str]]
    path: Optional[str]
    data: Optional[DataFrame]
    rejectCaptureFlag: str


@dataclass
class DataQualityConfig:
    db_name: str
    table_name: str
    column_name: str
    sql: str
    constraint_name: str
    constraint_rule: str
    check_level: str
    check_message: str
