import json
import os
import logging
from typing import Dict, Any, List
from datetime import datetime

from models.schemas import ValidationReport, CheckStatus, AnomalyRecord
from config import settings

logger = logging.getLogger(__name__)


class ReportBuilder:
    """Builds structured reports from validation results"""

    def __init__(self):
        self._reports: Dict[str, ValidationReport] = {}
        os.makedirs(settings.REPORT_DIR, exist_ok=True)

    def store_report(self, report: ValidationReport):
        """Store a validation report"""
        self._reports[report.report_id] = report

    def get_report(self, report_id: str) -> ValidationReport:
        """Retrieve a stored report"""
        if report_id not in self._reports:
            raise KeyError(f"Report '{report_id}' not found")
        return self._reports[report_id]

    def list_reports(self) -> List[Dict[str, Any]]:
        """List all reports"""
        return [
            {
                "report_id": r.report_id,
                "dataset_id": r.dataset_id,
                "overall_status": r.overall_status.value,
                "overall_score": r.overall_score,
                "generated_at": r.generated_at.isoformat(),
            }
            for r in self._reports.values()
        ]

    def build_summary(self, report: ValidationReport) -> Dict[str, Any]:
        """Build a human-readable summary"""

        # Column-level summary
        column_summary = {}
        for col, results in report.column_results.items():
            passed = sum(1 for r in results if r.status == CheckStatus.PASSED)
            failed = sum(1 for r in results if r.status == CheckStatus.FAILED)
            total = len(results)

            column_summary[col] = {
                "total_checks": total,
                "passed": passed,
                "failed": failed,
                "health": "HEALTHY" if failed == 0 else "UNHEALTHY",
                "checks": [
                    {
                        "rule_id": r.rule_id,
                        "rule_type": r.rule_type.value,
                        "status": r.status.value,
                        "pass_percentage": r.pass_percentage,
                        "failed_records": r.failed_records,
                        "severity": r.severity.value,
                        "description": r.description,
                    }
                    for r in results
                ]
            }

        return {
            "report_id": report.report_id,
            "dataset_id": report.dataset_id,
            "generated_at": report.generated_at.isoformat(),
            "execution_time_ms": report.execution_time_ms,

            "overall": {
                "status": report.overall_status.value,
                "score": report.overall_score,
                "verdict": (
                    "✅ DATA QUALITY PASSED"
                    if report.overall_status == CheckStatus.PASSED
                    else "⚠️ DATA QUALITY WARNING"
                    if report.overall_status == CheckStatus.WARNING
                    else "❌ DATA QUALITY FAILED"
                ),
                "total_rules": report.total_rules,
                "passed_rules": report.passed_rules,
                "failed_rules": report.failed_rules,
                "warning_rules": report.warning_rules,
            },

            "column_summary": column_summary,
            "anomaly_summary": report.anomaly_summary,
        }

    def build_anomaly_audit_trail(
        self,
        anomalies: List[AnomalyRecord]
    ) -> List[Dict[str, Any]]:
        """Convert anomalies to JSON audit trail"""
        return [
            {
                "anomaly_id": a.anomaly_id,
                "dataset_id": a.dataset_id,
                "rule_id": a.rule_id,
                "rule_type": a.rule_type,
                "column": a.column,
                "row_index": a.row_index,
                "actual_value": str(a.actual_value) if a.actual_value is not None else None,
                "expected_constraint": a.expected_constraint,
                "severity": a.severity,
                "detected_at": a.detected_at.isoformat(),
                "row_data": {
                    k: str(v) if v is not None else None
                    for k, v in a.row_data.items()
                },
            }
            for a in anomalies
        ]

    def export_report_json(self, report: ValidationReport, anomalies: List[AnomalyRecord]) -> str:
        """Export full report as JSON file"""
        summary = self.build_summary(report)
        summary["audit_trail"] = self.build_anomaly_audit_trail(anomalies)

        filepath = os.path.join(
            settings.REPORT_DIR,
            f"{report.report_id}_{report.dataset_id}.json"
        )
        with open(filepath, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        logger.info(f"Report exported to: {filepath}")
        return filepath


# Global singleton
report_builder = ReportBuilder()
