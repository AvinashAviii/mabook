import logging
from fastapi import APIRouter, HTTPException

from reporting.report_builder import report_builder
from engine.rule_executor import rule_executor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["Reports"])


@router.get("/")
async def list_reports():
    """List all validation reports"""
    return {"reports": report_builder.list_reports()}


@router.get("/{report_id}")
async def get_report(report_id: str):
    """Get a specific validation report"""
    try:
        report = report_builder.get_report(report_id)
        return report_builder.build_summary(report)
    except KeyError:
        raise HTTPException(404, f"Report '{report_id}' not found")


@router.get("/{report_id}/anomalies")
async def get_report_anomalies(report_id: str):
    """Get anomaly audit trail for a report"""
    try:
        report = report_builder.get_report(report_id)
        anomalies = rule_executor.get_anomalies(report.dataset_id)
        audit_trail = report_builder.build_anomaly_audit_trail(anomalies)
        return {
            "report_id": report_id,
            "dataset_id": report.dataset_id,
            "total_anomalies": len(audit_trail),
            "audit_trail": audit_trail,
        }
    except KeyError:
        raise HTTPException(404, f"Report '{report_id}' not found")


@router.get("/alerts/history")
async def get_alert_history():
    """Get history of all alerts sent"""
    from alerts.alert_engine import alert_engine
    return {"alerts": alert_engine.get_alert_history()}
