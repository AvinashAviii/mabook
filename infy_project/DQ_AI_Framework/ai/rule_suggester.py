import json
import logging
import uuid
from typing import Dict, Any, List
from datetime import datetime

from ai.gemini_client import gemini_client
from ai.prompts import RULE_SUGGESTION_PROMPT
from models.schemas import DQRule, DQRuleSet, RuleType, Severity

logger = logging.getLogger(__name__)


class RuleSuggester:
    """Uses Gemini AI to analyze data profiles and suggest DQ rules"""

    def suggest_rules(
        self,
        dataset_id: str,
        metadata: Dict[str, Any],
        context: str = "",
        max_rules: int = 50
    ) -> Dict[str, Any]:
        """
        Analyze dataset profile and generate DQ rules using Gemini AI.

        Returns dict with 'ruleset', 'reasoning', 'observations'
        """

        if not gemini_client.is_available:
            logger.warning("Gemini not available, generating heuristic rules")
            return self._generate_heuristic_rules(dataset_id, metadata)

        # Build prompt
        prompt = self._build_prompt(metadata, context)

        # Call Gemini
        logger.info(
            f"Requesting AI rule suggestions for dataset '{dataset_id}'")
        ai_response = gemini_client.generate(prompt)

        # Parse and validate rules
        rules = self._parse_ai_rules(ai_response, dataset_id, max_rules)

        ruleset = DQRuleSet(
            dataset_id=dataset_id,
            rules=rules,
            generated_by="gemini_ai",
            generated_at=datetime.utcnow(),
            metadata={
                "model": "gemini-1.5-flash",
                "context": context,
                "total_suggested": len(rules),
            }
        )

        return {
            "ruleset": ruleset,
            "reasoning": ai_response.get("reasoning", ""),
            "observations": ai_response.get("business_observations", []),
        }

    def _build_prompt(self, metadata: Dict[str, Any], context: str) -> str:
        """Construct the prompt from metadata"""

        # Schema profile
        schema_lines = []
        for col_name, profile in metadata.get("column_profiles", {}).items():
            schema_lines.append(
                f"  - {col_name}: type={profile['data_type']}, "
                f"nulls={profile['null_percentage']}%, "
                f"distinct={profile['distinct_count']}, "
                f"uniqueness={profile['uniqueness_ratio']}%"
            )
        schema_profile = "\n".join(schema_lines)

        # Column statistics (detailed)
        col_stats_lines = []
        for col_name, profile in metadata.get("column_profiles", {}).items():
            stats = f"Column '{col_name}':\n"
            for k, v in profile.items():
                if k != "top_values":
                    stats += f"    {k}: {v}\n"
                else:
                    stats += f"    top_values: {json.dumps(v, default=str)}\n"
            col_stats_lines.append(stats)
        column_stats = "\n".join(col_stats_lines)

        # Sample data
        sample_data = json.dumps(
            metadata.get("sample_data", []),
            indent=2,
            default=str
        )

        prompt = RULE_SUGGESTION_PROMPT.format(
            context=context or "Employee/HR data table",
            schema_profile=schema_profile,
            sample_data=sample_data,
            column_stats=column_stats,
        )

        return prompt

    def _parse_ai_rules(
        self,
        ai_response: dict,
        dataset_id: str,
        max_rules: int
    ) -> List[DQRule]:
        """Parse and validate AI-generated rules"""
        rules = []
        raw_rules = ai_response.get("rules", [])

        for i, raw in enumerate(raw_rules[:max_rules]):
            try:
                rule = DQRule(
                    rule_id=raw.get("rule_id", f"AI_R{i+1:03d}"),
                    rule_type=RuleType(raw["rule_type"]),
                    column=raw["column"],
                    severity=Severity(raw.get("severity", "medium")),
                    description=raw.get("description", ""),
                    params=raw.get("params", {}),
                    active=True,
                )
                rules.append(rule)
            except Exception as e:
                logger.warning(
                    f"Skipping invalid AI rule {i}: {e} | Raw: {raw}")
                continue

        logger.info(
            f"Parsed {len(rules)} valid rules from {len(raw_rules)} AI suggestions")
        return rules

    def _generate_heuristic_rules(
        self,
        dataset_id: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fallback: generate rules using statistical heuristics"""

        rules = []
        rule_counter = 0

        for col_name, profile in metadata.get("column_profiles", {}).items():
            col_type = profile.get("data_type", "")

            # Not Null check if < 5% nulls
            if profile.get("null_percentage", 0) < 5:
                rule_counter += 1
                rules.append(DQRule(
                    rule_id=f"H_R{rule_counter:03d}",
                    rule_type=RuleType.NOT_NULL,
                    column=col_name,
                    severity=Severity.HIGH if profile["null_percentage"] == 0 else Severity.MEDIUM,
                    description=f"'{col_name}' should not be null (observed {profile['null_percentage']}% nulls)",
                    params={},
                ))

            # Uniqueness check for high uniqueness columns
            if profile.get("uniqueness_ratio", 0) > 95:
                rule_counter += 1
                rules.append(DQRule(
                    rule_id=f"H_R{rule_counter:03d}",
                    rule_type=RuleType.UNIQUENESS,
                    column=col_name,
                    severity=Severity.CRITICAL,
                    description=f"'{col_name}' appears to be a unique identifier ({profile['uniqueness_ratio']}% unique)",
                    params={},
                ))

            # Domain check for low-cardinality string columns
            if "StringType" in col_type and profile.get("distinct_count", 999) <= 20:
                top_vals = profile.get("top_values", {})
                if top_vals:
                    rule_counter += 1
                    rules.append(DQRule(
                        rule_id=f"H_R{rule_counter:03d}",
                        rule_type=RuleType.DOMAIN_CHECK,
                        column=col_name,
                        severity=Severity.MEDIUM,
                        description=f"'{col_name}' should be within known domain values",
                        params={"allowed_values": list(top_vals.keys())},
                    ))

            # Range check for numeric columns
            if "IntegerType" in col_type or "DoubleType" in col_type or "LongType" in col_type:
                min_val = profile.get("min_value")
                max_val = profile.get("max_value")
                if min_val is not None and max_val is not None:
                    rule_counter += 1
                    # Add 10% buffer
                    buffer = abs(max_val - min_val) * \
                        0.1 if max_val != min_val else 1
                    rules.append(DQRule(
                        rule_id=f"H_R{rule_counter:03d}",
                        rule_type=RuleType.RANGE_CHECK,
                        column=col_name,
                        severity=Severity.MEDIUM,
                        description=f"'{col_name}' should be between {min_val} and {max_val}",
                        params={
                            "min_value": min_val - buffer,
                            "max_value": max_val + buffer,
                        },
                    ))

            # Length check for string columns
            if "StringType" in col_type:
                min_len = profile.get("min_length")
                max_len = profile.get("max_length")
                if min_len is not None and max_len is not None:
                    rule_counter += 1
                    rules.append(DQRule(
                        rule_id=f"H_R{rule_counter:03d}",
                        rule_type=RuleType.LENGTH_CHECK,
                        column=col_name,
                        severity=Severity.LOW,
                        description=f"'{col_name}' length should be between {min_len} and {max_len}",
                        params={"min_length": max(
                            1, min_len), "max_length": max_len + 10},
                    ))

        ruleset = DQRuleSet(
            dataset_id=dataset_id,
            rules=rules,
            generated_by="heuristic_fallback",
            generated_at=datetime.utcnow(),
        )

        return {
            "ruleset": ruleset,
            "reasoning": "Generated using statistical heuristics (Gemini unavailable)",
            "observations": ["Rules generated from data profiling statistics"],
        }


# Global singleton
rule_suggester = RuleSuggester()
