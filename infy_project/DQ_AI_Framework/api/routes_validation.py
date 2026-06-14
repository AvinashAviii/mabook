import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks

from core.data_loader import data_loader
from engine.rule_executor import rule_executor
from reporting.report_builder import report_builder
from alerts.alert_engine import alert_engine
from models.schemas import (
    ValidateRequest, DQRule, DQRuleSet, ValidationReport
)
from api.routes_analysis import get_stored_ruleset, store_ruleset

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/validate", tags=["Validation"])


@router.post("/run")
async def run_validation(
    request: ValidateRequest,
    background_tasks: BackgroundTasks,
):
    """
    Execute DQ validation rules against a dataset.

    - Uses AI-suggested rules or custom rules
    - Runs all checks via PySpark
    - Generates structured report
    - Triggers async alerts for failures
    """
    # Get dataset
    try:
        df = data_loader.get_dataset(request.dataset_id)
    except KeyError:
        raise HTTPException(404, f"Dataset '{request.dataset_id}' not found")

    # Get or build ruleset
    if request.custom_rules:
        ruleset = DQRuleSet(
            dataset_id=request.dataset_id,
            rules=request.custom_rules,
            generated_by="manual",
        )
        store_ruleset(request.dataset_id, ruleset)
    else:
        try:
            ruleset = get_stored_ruleset(request.dataset_id)
        except KeyError:
            raise HTTPException(
                400,
                f"No rules available for '{request.dataset_id}'. "
                "Either suggest rules via /analyze/suggest-rules or provide custom_rules."
            )

    # Execute validation
    try:
        report = rule_executor.execute_ruleset(
            df=df,
            ruleset=ruleset,
            fail_threshold=request.fail_threshold,
        )
    except Exception as e:
        logger.error(f"Validation execution failed: {e}", exc_info=True)
        raise HTTPException(500, f"Validation failed: {str(e)}")

    # Store report
    report_builder.store_report(report)

    # Async alert if failures detected
    if alert_engine.should_alert(report):
        background_tasks.add_task(
            alert_engine.send_alert,
            report,
            ["log", "email", "sns"]
        )
        logger.info(f"Alert queued for report {report.report_id}")

    # Build response
    summary = report_builder.build_summary(report)

    # Get anomalies
    anomalies = rule_executor.get_anomalies(request.dataset_id)
    audit_trail = report_builder.build_anomaly_audit_trail(anomalies)

    # Export to file
    try:
        filepath = report_builder.export_report_json(report, anomalies)
        summary["export_path"] = filepath
    except Exception as e:
        logger.warning(f"Report export failed: {e}")

    return {
        **summary,
        "anomaly_audit_trail": audit_trail[:50],  # limit in response
        "total_anomalies_captured": len(audit_trail),
    }


@router.post("/quick-check")
async def quick_check(
    dataset_id: str,
    background_tasks: BackgroundTasks,
):
    """
    Quick validation: auto-suggest rules and run immediately.
    Combines analysis + validation in one call.
    """
    from ai.rule_suggester import rule_suggester

    # Check dataset exists
    try:
        df = data_loader.get_dataset(dataset_id)
        metadata = data_loader.get_metadata(dataset_id)
    except KeyError:
        raise HTTPException(404, f"Dataset '{dataset_id}' not found")

    # AI suggest rules
    result = rule_suggester.suggest_rules(
        dataset_id=dataset_id,
        metadata=metadata,
        context="Employee data table - quick quality check",
    )
    ruleset = result["ruleset"]
    store_ruleset(dataset_id, ruleset)

    # Execute
    report = rule_executor.execute_ruleset(df=df, ruleset=ruleset)
    report_builder.store_report(report)

    # Alert
    if alert_engine.should_alert(report):
        background_tasks.add_task(alert_engine.send_alert, report, ["log"])

    summary = report_builder.build_summary(report)
    anomalies = rule_executor.get_anomalies(dataset_id)

    return {
        "ai_rules_count": len(ruleset.rules),
        "ai_reasoning": result.get("reasoning", ""),
        **summary,
        "anomalies_sample": report_builder.build_anomaly_audit_trail(anomalies[:20]),
    }
