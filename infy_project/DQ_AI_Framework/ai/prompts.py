RULE_SUGGESTION_PROMPT = """
You are a Senior Data Quality Engineer. Analyze the following dataset profile 
and suggest comprehensive data quality rules.

## Dataset Context
{context}

## Schema & Profile
{schema_profile}

## Sample Data (first 5 rows)
{sample_data}

## Column Statistics
{column_stats}

## Instructions

Generate a JSON object with the following structure:

{{
  "rules": [
    {{
      "rule_id": "R001",
      "rule_type": "<one of: not_null, length_check, range_check, domain_check, regex_check, uniqueness, data_type, conditional, custom_sql>",
      "column": "<column_name>",
      "severity": "<one of: critical, high, medium, low, info>",
      "description": "<human-readable description of what this rule checks>",
      "params": {{
        // Type-specific parameters:
        // not_null: {{}}
        // length_check: {{"min_length": int, "max_length": int}}
        // range_check: {{"min_value": number, "max_value": number}}
        // domain_check: {{"allowed_values": [list]}}
        // regex_check: {{"pattern": "regex_string"}}
        // uniqueness: {{}}
        // data_type: {{"expected_type": "string|integer|double|date"}}
        // conditional: {{
        //   "condition_column": "col_name",
        //   "condition_operator": "equals|not_equals|in|greater_than",
        //   "condition_value": "value or [list]",
        //   "then_rule_type": "not_null|domain_check|range_check|...",
        //   "then_params": {{...}}
        // }}
      }}
    }}
  ],
  "reasoning": "<Explain your analysis approach and why these rules were chosen>",
  "business_observations": [
    "<observation 1>",
    "<observation 2>"
  ]
}}

## IMPORTANT RULES:
1. Suggest NOT NULL checks for columns that appear to be mandatory (0% or very low nulls)
2. Suggest UNIQUENESS for columns that look like IDs
3. Suggest DOMAIN checks when you see a limited set of categorical values
4. Suggest RANGE checks for numeric columns based on observed min/max
5. Suggest LENGTH checks for string columns with consistent lengths
6. Suggest REGEX checks for patterns like email, phone, SSN, dates
7. Suggest CONDITIONAL rules for business logic (e.g., "if department=IT, role must be in [Engineer, Analyst]")
8. Set severity=critical for primary keys and essential business fields
9. Be thorough but avoid redundant rules
10. Generate at least one conditional rule if the data warrants it

Return ONLY valid JSON. No markdown, no explanation outside JSON.
"""


ANOMALY_ANALYSIS_PROMPT = """
You are a Data Quality Analyst. Analyze these failed records and provide insights.

## Rule That Failed
{rule_description}

## Failed Records Sample
{failed_records}

## Dataset Overview
{dataset_overview}

Generate a JSON response:
{{
  "root_cause_hypothesis": "<what likely caused these failures>",
  "pattern_detected": "<any pattern in the failures>",
  "recommended_action": "<what should be done to fix>",
  "is_data_issue": true/false,
  "is_rule_too_strict": true/false,
  "confidence": "<high|medium|low>"
}}