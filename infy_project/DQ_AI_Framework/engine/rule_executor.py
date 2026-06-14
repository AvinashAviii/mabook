import time
import uuid
import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from models.schemas import (
    DQRule, DQRuleSet, RuleType, Severity,
    CheckStatus, ColumnCheckResult, ValidationReport,
    AnomalyRecord
)
from config import settings

logger = logging.getLogger(__name__)


class RuleExecutor:
    """Executes DQ rules against PySpark DataFrames"""

    def __init__(self):
        self._anomalies: Dict[str, List[AnomalyRecord]] = {}

    def execute_ruleset(
        self,
        df: SparkDataFrame,
        ruleset: DQRuleSet,
        fail_threshold: float = None
    ) -> ValidationReport:
        """Execute all rules in a ruleset against a DataFrame"""

        start_time = time.time()
        fail_threshold = fail_threshold or settings.DQ_PASS_THRESHOLD

        report_id = f"RPT_{uuid.uuid4().hex[:8]}"
        dataset_id = ruleset.dataset_id
        total_records = df.count()

        column_results: Dict[str, List[ColumnCheckResult]] = {}
        all_results: List[ColumnCheckResult] = []
        all_anomalies: List[AnomalyRecord] = []

        logger.info(
            f"Executing {len(ruleset.rules)} rules on dataset '{dataset_id}' "
            f"({total_records} records)"
        )

        # Add row index for anomaly tracking
        df_indexed = df.withColumn(
            "__row_idx__",
            F.monotonically_increasing_id()
        )

        for rule in ruleset.rules:
            if not rule.active:
                continue

            try:
                rule_start = time.time()
                result, anomalies = self._execute_single_rule(
                    df_indexed, rule, total_records, dataset_id
                )
                result.execution_time_ms = round(
                    (time.time() - rule_start) * 1000, 2)

                all_results.append(result)
                all_anomalies.extend(anomalies)

                col = rule.column
                if col not in column_results:
                    column_results[col] = []
                column_results[col].append(result)

                status_icon = "✅" if result.status == CheckStatus.PASSED else "❌"
                logger.info(
                    f"  {status_icon} {rule.rule_id} [{rule.rule_type.value}] "
                    f"on '{rule.column}': {result.pass_percentage:.1f}% "
                    f"({result.status.value})"
                )

            except Exception as e:
                logger.error(f"  ⚠️ Rule {rule.rule_id} execution error: {e}")
                error_result = ColumnCheckResult(
                    rule_id=rule.rule_id,
                    rule_type=rule.rule_type,
                    column=rule.column,
                    status=CheckStatus.ERROR,
                    severity=rule.severity,
                    total_records=total_records,
                    passed_records=0,
                    failed_records=0,
                    pass_percentage=0,
                    description=f"Execution error: {str(e)}",
                )
                all_results.append(error_result)

        # Store anomalies
        self._anomalies[dataset_id] = all_anomalies

        # Calculate overall metrics
        passed = sum(1 for r in all_results if r.status == CheckStatus.PASSED)
        failed = sum(1 for r in all_results if r.status == CheckStatus.FAILED)
        warnings = sum(1 for r in all_results if r.status ==
                       CheckStatus.WARNING)
        errors = sum(1 for r in all_results if r.status == CheckStatus.ERROR)

        total_rules = len(all_results)
        overall_score = round((passed / total_rules * 100)
                              if total_rules > 0 else 0, 2)

        # Determine overall status
        critical_failures = any(
            r.status == CheckStatus.FAILED and r.severity in (
                Severity.CRITICAL, Severity.HIGH)
            for r in all_results
        )

        if critical_failures or overall_score < fail_threshold:
            overall_status = CheckStatus.FAILED
        elif warnings > 0:
            overall_status = CheckStatus.WARNING
        else:
            overall_status = CheckStatus.PASSED

        execution_time = round((time.time() - start_time) * 1000, 2)

        report = ValidationReport(
            report_id=report_id,
            dataset_id=dataset_id,
            overall_status=overall_status,
            overall_score=overall_score,
            total_rules=total_rules,
            passed_rules=passed,
            failed_rules=failed,
            warning_rules=warnings,
            error_rules=errors,
            column_results=column_results,
            anomaly_summary={
                "total_anomalies": len(all_anomalies),
                "by_severity": self._count_by_severity(all_anomalies),
                "by_column": self._count_by_column(all_anomalies),
            },
            execution_time_ms=execution_time,
            metadata={
                "fail_threshold": fail_threshold,
                "total_records": total_records,
            }
        )

        logger.info(
            f"\n{'='*60}\n"
            f"Validation Report: {report_id}\n"
            f"Overall Status: {overall_status.value.upper()}\n"
            f"Score: {overall_score}% | "
            f"Passed: {passed}/{total_rules} | "
            f"Failed: {failed} | Warnings: {warnings}\n"
            f"Total Anomalies: {len(all_anomalies)}\n"
            f"Execution Time: {execution_time}ms\n"
            f"{'='*60}"
        )

        return report

    def _execute_single_rule(
        self,
        df: SparkDataFrame,
        rule: DQRule,
        total_records: int,
        dataset_id: str
    ) -> tuple:
        """Execute a single DQ rule and return result + anomalies"""

        executor_map = {
            RuleType.NOT_NULL: self._check_not_null,
            RuleType.LENGTH_CHECK: self._check_length,
            RuleType.RANGE_CHECK: self._check_range,
            RuleType.DOMAIN_CHECK: self._check_domain,
            RuleType.REGEX_CHECK: self._check_regex,
            RuleType.UNIQUENESS: self._check_uniqueness,
            RuleType.DATA_TYPE: self._check_data_type,
            RuleType.CONDITIONAL: self._check_conditional,
            RuleType.CUSTOM_SQL: self._check_custom_sql,
        }

        executor = executor_map.get(rule.rule_type)
        if not executor:
            raise ValueError(f"Unsupported rule type: {rule.rule_type}")

        # Get failed DataFrame
        failed_df = executor(df, rule)

        # Count failures
        failed_count = failed_df.count()
        passed_count = total_records - failed_count
        pass_pct = round((passed_count / total_records * 100)
                         if total_records > 0 else 100, 2)

        # Determine status
        if pass_pct >= 100:
            status = CheckStatus.PASSED
        elif pass_pct >= settings.DQ_PASS_THRESHOLD:
            status = CheckStatus.WARNING
        else:
            status = CheckStatus.FAILED

        # Collect failed samples for audit
        sample_size = min(settings.ANOMALY_SAMPLE_SIZE, failed_count)
        failed_sample = []
        anomalies = []

        if failed_count > 0:
            sample_rows = failed_df.limit(sample_size).collect()
            for row in sample_rows:
                row_dict = row.asDict()
                row_idx = row_dict.pop("__row_idx__", -1)

                # Clean non-serializable values
                cleaned = {}
                for k, v in row_dict.items():
                    if hasattr(v, 'isoformat'):
                        cleaned[k] = v.isoformat()
                    else:
                        cleaned[k] = v

                failed_sample.append(cleaned)

                anomalies.append(AnomalyRecord(
                    anomaly_id=f"ANO_{uuid.uuid4().hex[:8]}",
                    dataset_id=dataset_id,
                    rule_id=rule.rule_id,
                    rule_type=rule.rule_type.value,
                    column=rule.column,
                    row_index=row_idx,
                    actual_value=cleaned.get(rule.column),
                    expected_constraint=f"{rule.rule_type.value}: {rule.params}",
                    severity=rule.severity.value,
                    row_data=cleaned,
                ))

        result = ColumnCheckResult(
            rule_id=rule.rule_id,
            rule_type=rule.rule_type,
            column=rule.column,
            status=status,
            severity=rule.severity,
            total_records=total_records,
            passed_records=passed_count,
            failed_records=failed_count,
            pass_percentage=pass_pct,
            description=rule.description,
            failed_sample=failed_sample,
        )

        return result, anomalies

    # ─── Individual Check Implementations ────────────────────────────

    def _check_not_null(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where column IS NULL"""
        col = rule.column
        return df.filter(
            F.col(col).isNull() | (F.trim(F.col(col)) == "")
        )

    def _check_length(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where string length is out of range"""
        col = rule.column
        min_len = rule.params.get("min_length", 0)
        max_len = rule.params.get("max_length", 99999)

        return df.filter(
            F.col(col).isNotNull() &
            (
                (F.length(F.col(col)) < min_len) |
                (F.length(F.col(col)) > max_len)
            )
        )

    def _check_range(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where numeric value is out of range"""
        col = rule.column
        min_val = rule.params.get("min_value")
        max_val = rule.params.get("max_value")

        conditions = F.col(col).isNotNull()
        range_conditions = []

        if min_val is not None:
            range_conditions.append(F.col(col) < min_val)
        if max_val is not None:
            range_conditions.append(F.col(col) > max_val)

        if range_conditions:
            combined = range_conditions[0]
            for cond in range_conditions[1:]:
                combined = combined | cond
            return df.filter(conditions & combined)

        return df.limit(0)  # No conditions, no failures

    def _check_domain(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where value is not in allowed domain"""
        col = rule.column
        allowed = rule.params.get("allowed_values", [])

        if not allowed:
            return df.limit(0)

        return df.filter(
            F.col(col).isNotNull() &
            ~F.col(col).isin(allowed)
        )

    def _check_regex(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where value doesn't match regex pattern"""
        col = rule.column
        pattern = rule.params.get("pattern", ".*")

        return df.filter(
            F.col(col).isNotNull() &
            ~F.col(col).rlike(pattern)
        )

    def _check_uniqueness(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return duplicate rows"""
        col = rule.column

        # Find values that appear more than once
        window = Window.partitionBy(col)
        df_with_count = df.withColumn(
            "__dup_count__", F.count("*").over(window))
        duplicates = df_with_count.filter(
            F.col("__dup_count__") > 1).drop("__dup_count__")

        return duplicates

    def _check_data_type(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Return rows where value can't be cast to expected type"""
        col = rule.column
        expected = rule.params.get("expected_type", "string")

        type_map = {
            "integer": "int",
            "double": "double",
            "date": "date",
            "timestamp": "timestamp",
        }

        cast_type = type_map.get(expected, "string")

        if cast_type == "string":
            return df.limit(0)  # Everything is a string

        # Try casting; if cast returns null but original is not null, it's a type error
        return df.filter(
            F.col(col).isNotNull() &
            F.col(col).cast(cast_type).isNull()
        )

    def _check_conditional(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """
        Conditional business logic:
        "If column A has value X, then column B must satisfy constraint Y"
        """
        params = rule.params
        cond_col = params.get("condition_column")
        cond_op = params.get("condition_operator", "equals")
        cond_val = params.get("condition_value")
        then_type = params.get("then_rule_type")
        then_params = params.get("then_params", {})

        if not all([cond_col, cond_val, then_type]):
            logger.warning(f"Incomplete conditional rule: {rule.rule_id}")
            return df.limit(0)

        # Build condition filter
        if cond_op == "equals":
            condition = F.col(cond_col) == cond_val
        elif cond_op == "not_equals":
            condition = F.col(cond_col) != cond_val
        elif cond_op == "in":
            condition = F.col(cond_col).isin(
                cond_val if isinstance(cond_val, list) else [cond_val])
        elif cond_op == "greater_than":
            condition = F.col(cond_col) > cond_val
        elif cond_op == "less_than":
            condition = F.col(cond_col) < cond_val
        else:
            condition = F.col(cond_col) == cond_val

        # Apply condition - only check rows where condition is met
        conditional_df = df.filter(condition)

        if conditional_df.count() == 0:
            return df.limit(0)

        # Apply the "then" check on the filtered data
        then_rule = DQRule(
            rule_id=f"{rule.rule_id}_then",
            rule_type=RuleType(then_type),
            column=rule.column,
            severity=rule.severity,
            description=f"Conditional check: {rule.description}",
            params=then_params,
        )

        # Recursively execute the "then" rule
        executor_map = {
            RuleType.NOT_NULL: self._check_not_null,
            RuleType.DOMAIN_CHECK: self._check_domain,
            RuleType.RANGE_CHECK: self._check_range,
            RuleType.REGEX_CHECK: self._check_regex,
            RuleType.LENGTH_CHECK: self._check_length,
        }

        then_executor = executor_map.get(RuleType(then_type))
        if then_executor:
            return then_executor(conditional_df, then_rule)

        return df.limit(0)

    def _check_custom_sql(self, df: SparkDataFrame, rule: DQRule) -> SparkDataFrame:
        """Execute custom SQL-based check"""
        sql_expr = rule.params.get("sql_expression", "")
        if not sql_expr:
            return df.limit(0)

        # Register temp view
        view_name = f"__dq_temp_{rule.rule_id}__"
        df.createOrReplaceTempView(view_name)

        # The SQL should return rows that FAIL the check
        full_sql = sql_expr.replace("{table}", view_name)
        from core.spark_manager import spark_manager
        return spark_manager.spark.sql(full_sql)

    # ─── Helper Methods ──────────────────────────────────────────────

    def get_anomalies(self, dataset_id: str) -> List[AnomalyRecord]:
        """Retrieve anomalies for a dataset"""
        return self._anomalies.get(dataset_id, [])

    def _count_by_severity(self, anomalies: List[AnomalyRecord]) -> Dict[str, int]:
        counts = {}
        for a in anomalies:
            counts[a.severity] = counts.get(a.severity, 0) + 1
        return counts

    def _count_by_column(self, anomalies: List[AnomalyRecord]) -> Dict[str, int]:
        counts = {}
        for a in anomalies:
            counts[a.column] = counts.get(a.column, 0) + 1
        return counts


# Global singleton
rule_executor = RuleExecutor()
