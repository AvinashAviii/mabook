This Python-based Data Quality (DQ) framework is a modular system built to automate data validation using PySpark and PyDeequ[cite: 1]. Below is a detailed analysis of each file, followed by a step-by-step trace of how your specific `PART_NO` input is processed through the system.

### 1. Detailed File and Function Analysis

#### `dq_models.py` (The Blueprint)
*   **Purpose**: Centralizes data structures to prevent circular imports and ensure consistent data types[cite: 1].
*   **`DataQualityRequest`**: A dataclass that holds the runtime context, including environment (`env`), database (`dbName`), table (`tableName`), and any filter conditions[cite: 1].
*   **`DataQualityConfig`**: A dataclass used to map rows from the rules configuration table into structured Python objects for the validation engine[cite: 1].

#### `dq_utils.py` (The Infrastructure Layer)
*   **Purpose**: Acts as a bridge between the framework and external resources like AWS and system utilities[cite: 1].
*   **`DateFunctions.time_now`**: Generates formatted timestamps for audit logs and partitioning[cite: 1].
*   **`AthenaTableRefresh`**: Contains methods to refresh metadata in AWS Athena after DQ results are updated[cite: 1].
*   **`SNSUtil.publish_notification`**: Handles the logic for sending failure alerts to specified SNS topics[cite: 1].

#### `data_quality.py` (The Orchestrator)
*   **`main(args)`**: The primary entry point. It parses input arguments (like `config.json`), validates required parameters, and creates the `DataQualityRequest` object[cite: 1].
*   **`process_dq(dqr)`**: The central router. Depending on the `module` requested (e.g., `validation`, `profile`, `optimize`), it directs the execution to the appropriate sub-module[cite: 1].
*   **`delta_table_cleanup(db, tbl, spark)`**: Performs maintenance on Delta tables using `OPTIMIZE` and `VACUUM` to ensure query performance[cite: 1].

#### `data_quality_verifier.py` (The Validation Engine)
*   **`dq_verify(dqr, spark)`**: Coordinates the verification process. It loads the source data, fetches active rules, and manages the notification and anomaly capture calls[cite: 1].
*   **`_process_checks(df, dqr, rules, tbl_nm, spark)`**: **The most critical function.** It initializes a PyDeequ `Check` object and uses Python's `eval()` to dynamically chain `constraint_rule` strings into executable code[cite: 1].
*   **`_notify(dqr, failed_list, spark)`**: Scans the verification results for failures and batches them into consolidated SNS alerts[cite: 1].

#### `data_quality_anamoly.py` (The Error Handler)
*   **`capture_data_anamolies(dqr, failed_ds, spark)`**: Identifies the specific rows that failed quality checks[cite: 1].
*   **Logic**: It executes the `reject_sql` provided in your rules, converts the entire failing record into a JSON string, and appends it to a central rejects table for inspection[cite: 1].

#### `data_quality_profiler.py` & `data_quality_suggestion.py` (Discovery)
*   **`profile_table`**: Runs PyDeequ's `ColumnProfilerRunner` to calculate completeness and data types for columns[cite: 1].
*   **`dq_suggest`**: Uses PyDeequ's `ConstraintSuggestionRunner` to analyze data and suggest new quality rules based on patterns it finds[cite: 1].

#### `data_quality_suggestion_load.py` & `data_quality_profile_load.py` (Ingestion)
*   **`dq_load`**: Reads pipe-delimited CSV files from S3/local, validates that the schema matches the framework's expectations, and updates the internal rules tables[cite: 1].

---

### 2. Processing Flow: The `PART_NO` Rule Example
Here is exactly how the framework processes your sample input:
> `parts_battery_trn | bl_save_supplier_response | PART_NO | ... | .satisfies("PART_NO is not null","PART_NO is valid") | ... | Error`

#### Step 1: Initialization (`data_quality.py`)
The `main` function reads your `config.json` and sees that you want to run `validation` on the `parts_battery_trn` database[cite: 1]. It creates a `DataQualityRequest` and passes it to `process_dq`, which calls `DataQualityVerifier.dq_verify`[cite: 1].

#### Step 2: Data & Rule Loading (`data_quality_verifier.py`)
`dq_verify` connects to your Spark session and loads the table `parts_battery_trn.bl_save_supplier_response`[cite: 1]. It then fetches all active rules for this table from the `dq_metrics_trn.data_quality_manual_rules` table, including your `PART_NO` rule[cite: 1].

#### Step 3: Dynamic Rule Building (`data_quality_verifier.py`)
Inside `_process_checks`, the system creates a `Check` object[cite: 1]. It takes the string from your input:
`.satisfies("PART_NO is not null","PART_NO is valid")`
The script executes: `check_obj = eval(f"check_obj{rule_str}")`, which turns that text into a live PyDeequ validation[cite: 1].

#### Step 4: Verification Execution (`data_quality_verifier.py`)
The `VerificationSuite` runs. PyDeequ checks every row in the `PART_NO` column against the rule[cite: 1]. If a row is null, the result is marked as `Failure` and logged to the `data_quality_check_results` table[cite: 1].

#### Step 5: Anomaly Capture (`data_quality_anamoly.py`)
Because a failure occurred, `capture_data_anamolies` is triggered[cite: 1]. It pulls the `reject_sql` from your rule:
`SELECT * FROM parts_battery_trn.bl_save_supplier_response WHERE PART_NO IS NULL`[cite: 1].
It runs this query, converts the "bad" rows into JSON, and saves them to S3 for root-cause analysis[cite: 1].

#### Step 6: Final Alerting (`data_quality_verifier.py`)
Finally, `_notify` triggers an SNS alert to let the data engineering team know that the `PART_NO` check failed in the `parts_battery_trn` environment[cite: 1].