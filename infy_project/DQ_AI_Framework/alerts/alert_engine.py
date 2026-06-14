import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional
from datetime import datetime

from models.schemas import ValidationReport, CheckStatus
from config import settings

logger = logging.getLogger(__name__)


class AlertEngine:
    """Handles asynchronous alerting for DQ failures"""

    def __init__(self):
        self._alert_history: List[Dict[str, Any]] = []

    def should_alert(self, report: ValidationReport) -> bool:
        """Determine if an alert should be triggered"""
        return report.overall_status in (CheckStatus.FAILED, CheckStatus.WARNING)

    def send_alert(self, report: ValidationReport, channels: List[str] = None):
        """
        Main alert dispatch method - called as BackgroundTask.
        Simulates SNS/Email alerting.
        """
        channels = channels or ["email", "sns", "log"]

        alert_payload = self._build_alert_payload(report)
        logger.info(f"🔔 Dispatching alert for report {report.report_id}")

        for channel in channels:
            try:
                if channel == "email":
                    self._send_email_alert(alert_payload)
                elif channel == "sns":
                    self._send_sns_alert(alert_payload)
                elif channel == "log":
                    self._send_log_alert(alert_payload)
                elif channel == "slack":
                    self._send_slack_alert(alert_payload)

                self._alert_history.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "report_id": report.report_id,
                    "channel": channel,
                    "status": "sent",
                    "message": alert_payload["subject"],
                })

            except Exception as e:
                logger.error(f"Alert failed for channel '{channel}': {e}")
                self._alert_history.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "report_id": report.report_id,
                    "channel": channel,
                    "status": "failed",
                    "error": str(e),
                })

    def _build_alert_payload(self, report: ValidationReport) -> Dict[str, Any]:
        """Build structured alert payload"""

        status_emoji = {
            CheckStatus.PASSED: "✅",
            CheckStatus.FAILED: "❌",
            CheckStatus.WARNING: "⚠️",
        }

        emoji = status_emoji.get(report.overall_status, "ℹ️")

        # Build failure details
        failure_details = []
        for col, results in report.column_results.items():
            for r in results:
                if r.status == CheckStatus.FAILED:
                    failure_details.append(
                        f"  - [{r.severity.value.upper()}] {r.rule_id}: "
                        f"'{r.column}' {r.rule_type.value} → "
                        f"{r.pass_percentage:.1f}% pass ({r.failed_records} failures)"
                    )

        return {
            "subject": (
                f"{emoji} DQ Alert: Dataset '{report.dataset_id}' - "
                f"{report.overall_status.value.upper()}"
            ),
            "report_id": report.report_id,
            "dataset_id": report.dataset_id,
            "status": report.overall_status.value,
            "score": report.overall_score,
            "total_rules": report.total_rules,
            "failed_rules": report.failed_rules,
            "failure_details": failure_details,
            "anomaly_count": report.anomaly_summary.get("total_anomalies", 0),
            "timestamp": datetime.utcnow().isoformat(),
            "body_text": self._format_text_body(report, failure_details),
            "body_html": self._format_html_body(report, failure_details),
        }

    def _format_text_body(
        self, report: ValidationReport, failure_details: List[str]
    ) -> str:
        return f"""
DATA QUALITY ALERT
==================
Report ID:  {report.report_id}
Dataset:    {report.dataset_id}
Status:     {report.overall_status.value.upper()}
Score:      {report.overall_score}%
Rules:      {report.passed_rules}/{report.total_rules} passed
Anomalies:  {report.anomaly_summary.get('total_anomalies', 0)}
Time:       {report.generated_at.isoformat()}

FAILURES:
{chr(10).join(failure_details) if failure_details else '  None'}

---
DQ Framework v{settings.APP_VERSION}
"""

    def _format_html_body(
        self, report: ValidationReport, failure_details: List[str]
    ) -> str:
        status_color = {
            CheckStatus.PASSED: "#28a745",
            CheckStatus.FAILED: "#dc3545",
            CheckStatus.WARNING: "#ffc107",
        }
        color = status_color.get(report.overall_status, "#6c757d")

        failures_html = ""
        if failure_details:
            failures_html = "<ul>" + "".join(
                f"<li>{d}</li>" for d in failure_details
            ) + "</ul>"
        else:
            failures_html = "<p>No failures detected.</p>"

        return f"""
<html>
<body style="font-family: Arial, sans-serif;">
<div style="max-width: 600px; margin: 0 auto; padding: 20px;">
    <h2 style="color: {color};">Data Quality Alert</h2>
    <table style="width: 100%; border-collapse: collapse;">
        <tr><td><strong>Report ID:</strong></td><td>{report.report_id}</td></tr>
        <tr><td><strong>Dataset:</strong></td><td>{report.dataset_id}</td></tr>
        <tr><td><strong>Status:</strong></td>
            <td style="color: {color}; font-weight: bold;">
                {report.overall_status.value.upper()}
            </td></tr>
        <tr><td><strong>Score:</strong></td><td>{report.overall_score}%</td></tr>
        <tr><td><strong>Rules Passed:</strong></td>
            <td>{report.passed_rules}/{report.total_rules}</td></tr>
        <tr><td><strong>Anomalies:</strong></td>
            <td>{report.anomaly_summary.get('total_anomalies', 0)}</td></tr>
    </table>
    <h3>Failure Details</h3>
    {failures_html}
    <hr/>
    <p style="color: #888; font-size: 12px;">
        DQ Framework v{settings.APP_VERSION} | {report.generated_at.isoformat()}
    </p>
</div>
</body>
</html>
"""

    def _send_email_alert(self, payload: Dict[str, Any]):
        """Send email alert (simulated if SMTP not configured)"""
        if not settings.SMTP_USER:
            logger.info(
                f"📧 [SIMULATED EMAIL]\n"
                f"  To: {settings.ALERT_RECIPIENTS}\n"
                f"  Subject: {payload['subject']}\n"
                f"  Body:\n{payload['body_text']}"
            )
            return

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = payload["subject"]
            msg["From"] = settings.SMTP_USER
            msg["To"] = ", ".join(settings.ALERT_RECIPIENTS)

            msg.attach(MIMEText(payload["body_text"], "plain"))
            msg.attach(MIMEText(payload["body_html"], "html"))

            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                server.starttls()
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.send_message(msg)

            logger.info(f"📧 Email sent to {settings.ALERT_RECIPIENTS}")

        except Exception as e:
            logger.error(f"Email send failed: {e}")
            raise

    def _send_sns_alert(self, payload: Dict[str, Any]):
        """Send AWS SNS alert (simulated if not configured)"""
        if not settings.SNS_TOPIC_ARN:
            logger.info(
                f"📱 [SIMULATED SNS]\n"
                f"  Topic: {settings.SNS_TOPIC_ARN or 'NOT_CONFIGURED'}\n"
                f"  Subject: {payload['subject']}\n"
                f"  Message: {payload['body_text'][:500]}"
            )
            return

        try:
            import boto3
            client = boto3.client("sns", region_name=settings.AWS_REGION)
            response = client.publish(
                TopicArn=settings.SNS_TOPIC_ARN,
                Subject=payload["subject"][:100],  # SNS subject limit
                Message=json.dumps(payload, indent=2, default=str),
            )
            logger.info(f"📱 SNS published: {response['MessageId']}")

        except Exception as e:
            logger.error(f"SNS publish failed: {e}")
            raise

    def _send_slack_alert(self, payload: Dict[str, Any]):
        """Send Slack webhook alert (simulated)"""
        logger.info(
            f"💬 [SIMULATED SLACK]\n"
            f"  {payload['subject']}\n"
            f"  Score: {payload['score']}%"
        )

    def _send_log_alert(self, payload: Dict[str, Any]):
        """Log-based alert (always works)"""
        logger.warning(
            f"\n{'🔔'*20}\n"
            f"ALERT: {payload['subject']}\n"
            f"Score: {payload['score']}% | "
            f"Failed: {payload['failed_rules']} rules\n"
            f"Anomalies: {payload['anomaly_count']}\n"
            f"{'🔔'*20}"
        )

    def get_alert_history(self) -> List[Dict[str, Any]]:
        """Get all alert history"""
        return self._alert_history


# Global singleton
alert_engine = AlertEngine()
