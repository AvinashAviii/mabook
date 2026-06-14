from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime


# ─── Enums ───────────────────────────────────────────────────────────

class RuleType(str, Enum):
    NOT_NULL = "not_null"
    LENGTH_CHECK = "length_check"
    RANGE_CHECK = "range_check"
    DOMAIN_CHECK = "domain_check"
    REGEX_CHECK = "regex_check"
    UNIQUENESS = "uniqueness"
    DATA_TYPE = "data_type"
    CONDITIONAL = "conditional"
    CUSTOM_SQL = "custom_sql"
    REFERENTIAL = "referential"
    COMPLETENESS = "completeness"
    FRESHNESS = "freshness"


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"
    ERROR = "error"


class DatasetStatus(str, Enum):
    UPLOADED = "uploaded"
    ANALYZED = "analyzed"
    VALIDATED = "validated"
    REPORTED = "reported"


# ─── Rule Definitions ───────────────────────────────────────────────

class DQRule(BaseModel):
    """Single data quality rule definition"""
    rule_id: str = Field(..., description="Unique rule identifier")
    rule_type: RuleType
    column: str = Field(..., description="Target column name")
    severity: Severity = Severity.HIGH
    description: str = ""
    active: bool = True

    # Type-specific parameters
    params: Dict[str, Any] = Field(default_factory=dict)
    # Examples:
    # not_null: {}
    # length_check: {"min_length": 1, "max_length": 50}
    # range_check: {"min_value": 0, "max_value": 150}
    # domain_check: {"allowed_values": ["M", "F", "Other"]}
    # regex_check: {"pattern": "^[A-Z]{2}\\d{6}$"}
    # conditional: {"condition_column": "dept", "condition_value": "IT",
    #               "then_check": {"rule_type": "domain_check",
    #                              "allowed_values": ["Engineer", "Analyst"]}}

    class Config:
        json_schema_extra = {
            "example": {
                "rule_id": "R001",
                "rule_type": "not_null",
                "column": "employee_id",
                "severity": "critical",
                "description": "Employee ID must not be null",
                "params": {}
            }
        }


class DQRuleSet(BaseModel):
    """Collection of DQ rules for a dataset"""
    dataset_id: str
    rules: List[DQRule]
    generated_by: str = "manual"  # "manual" or "ai"
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ─── Validation Results ─────────────────────────────────────────────

class ColumnCheckResult(BaseModel):
    """Result of a single rule check on a column"""
    rule_id: str
    rule_type: RuleType
    column: str
    status: CheckStatus
    severity: Severity
    total_records: int
    passed_records: int
    failed_records: int
    pass_percentage: float
    description: str
    failed_sample: List[Dict[str, Any]] = []  # sample of failed rows
    execution_time_ms: float = 0


class ValidationReport(BaseModel):
    """Complete validation report for a dataset"""
    report_id: str
    dataset_id: str
    overall_status: CheckStatus
    overall_score: float
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    error_rules: int

    column_results: Dict[str, List[ColumnCheckResult]]
    anomaly_summary: Dict[str, Any] = {}
    execution_time_ms: float

    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ─── API Request/Response Models ────────────────────────────────────

class UploadResponse(BaseModel):
    dataset_id: str
    filename: str
    row_count: int
    column_count: int
    columns: List[str]
    dtypes: Dict[str, str]
    sample_data: List[Dict[str, Any]]
    status: str = "uploaded"


class AnalyzeRequest(BaseModel):
    dataset_id: str
    context: Optional[str] = Field(
        None,
        description="Business context, e.g., 'This is an HR employee table'"
    )
    include_conditional: bool = True
    max_rules: int = 50


class AnalyzeResponse(BaseModel):
    dataset_id: str
    suggested_rules: DQRuleSet
    ai_reasoning: str
    data_profile: Dict[str, Any]


class ValidateRequest(BaseModel):
    dataset_id: str
    rule_set_id: Optional[str] = None
    custom_rules: Optional[List[DQRule]] = None
    fail_threshold: float = 95.0


class AlertConfig(BaseModel):
    enabled: bool = True
    channels: List[str] = ["email"]  # "email", "sns", "slack"
    recipients: List[str] = []
    on_failure_only: bool = True


class AnomalyRecord(BaseModel):
    """Single anomaly record for audit trail"""
    anomaly_id: str
    dataset_id: str
    rule_id: str
    rule_type: str
    column: str
    row_index: int
    actual_value: Any
    expected_constraint: str
    severity: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    row_data: Dict[str, Any] = {}
