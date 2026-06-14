import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


class TestHealthEndpoints:
    def test_root(self):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"

    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestDemoWorkflow:
    """Tests the complete workflow: upload → analyze → validate → report"""

    def test_01_upload_demo(self):
        response = client.post("/data/upload/demo")
        assert response.status_code == 200
        data = response.json()
        assert data["dataset_id"] == "demo_employees"
        assert data["row_count"] == 10
        assert "employee_id" in data["columns"]

    def test_02_list_datasets(self):
        # Ensure demo is uploaded first
        client.post("/data/upload/demo")
        response = client.get("/data/datasets")
        assert response.status_code == 200
        datasets = response.json()["datasets"]
        assert len(datasets) > 0

    def test_03_get_sample(self):
        client.post("/data/upload/demo")
        response = client.get("/data/datasets/demo_employees/sample?n=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) <= 5

    def test_04_suggest_rules(self):
        client.post("/data/upload/demo")
        response = client.post("/analyze/suggest-rules", json={
            "dataset_id": "demo_employees",
            "context": "HR employee data with intentional quality issues",
            "max_rules": 20,
        })
        assert response.status_code == 200
        data = response.json()
        assert data["total_rules_suggested"] > 0
        assert len(data["rules"]) > 0

    def test_05_run_validation(self):
        # Upload + suggest first
        client.post("/data/upload/demo")
        client.post("/analyze/suggest-rules", json={
            "dataset_id": "demo_employees",
            "context": "Employee data",
        })

        response = client.post("/validate/run", json={
            "dataset_id": "demo_employees",
            "fail_threshold": 95.0,
        })
        assert response.status_code == 200
        data = response.json()
        assert "overall" in data
        assert "column_summary" in data
        assert data["overall"]["total_rules"] > 0

    def test_06_quick_check(self):
        client.post("/data/upload/demo")
        response = client.post(
            "/validate/quick-check?dataset_id=demo_employees")
        assert response.status_code == 200
        data = response.json()
        assert "overall" in data
        assert "ai_rules_count" in data

    def test_07_custom_rules(self):
        client.post("/data/upload/demo")
        custom_rules = [
            {
                "rule_id": "CUSTOM_001",
                "rule_type": "not_null",
                "column": "employee_id",
                "severity": "critical",
                "description": "Employee ID must not be null",
                "params": {}
            },
            {
                "rule_id": "CUSTOM_002",
                "rule_type": "range_check",
                "column": "age",
                "severity": "high",
                "description": "Age must be between 18 and 100",
                "params": {"min_value": 18, "max_value": 100}
            },
            {
                "rule_id": "CUSTOM_003",
                "rule_type": "domain_check",
                "column": "department",
                "severity": "medium",
                "description": "Department must be valid",
                "params": {
                    "allowed_values": [
                        "Engineering", "Marketing", "HR", "Sales", "Finance"
                    ]
                }
            },
            {
                "rule_id": "CUSTOM_004",
                "rule_type": "conditional",
                "column": "role",
                "severity": "high",
                "description": "If dept=Engineering, role must be Engineer or Senior Engineer",
                "params": {
                    "condition_column": "department",
                    "condition_operator": "equals",
                    "condition_value": "Engineering",
                    "then_rule_type": "domain_check",
                    "then_params": {
                        "allowed_values": ["Engineer", "Senior Engineer", "Tech Lead"]
                    }
                }
            },
            {
                "rule_id": "CUSTOM_005",
                "rule_type": "regex_check",
                "column": "email",
                "severity": "high",
                "description": "Email must be valid format",
                "params": {
                    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                }
            }
        ]

        response = client.post("/validate/run", json={
            "dataset_id": "demo_employees",
            "custom_rules": custom_rules,
            "fail_threshold": 90.0,
        })
        assert response.status_code == 200
        data = response.json()

        # Verify we have results
        assert data["overall"]["total_rules"] == 5
        # Demo data has intentional issues
        assert data["overall"]["failed_rules"] > 0
        assert data["total_anomalies_captured"] > 0

    def test_08_list_reports(self):
        response = client.get("/reports/")
        assert response.status_code == 200

    def test_09_alert_history(self):
        response = client.get("/reports/alerts/history")
        assert response.status_code == 200


class TestErrorHandling:
    def test_dataset_not_found(self):
        response = client.get("/data/datasets/nonexistent")
        assert response.status_code == 404

    def test_validate_without_rules(self):
        client.post("/data/upload/demo")
        # Clear any existing rules by using a new dataset concept
        response = client.post("/validate/run", json={
            "dataset_id": "nonexistent_dataset",
        })
        assert response.status_code == 404

    def test_invalid_file_type(self):
        import io
        response = client.post(
            "/data/upload",
            files={"file": ("test.txt", io.BytesIO(b"hello"), "text/plain")},
        )
        assert response.status_code == 400
