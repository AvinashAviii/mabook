import logging
from typing import Dict, Any

from fastapi import APIRouter, HTTPException

from core.data_loader import data_loader
from ai.rule_suggester import rule_suggester
from models.schemas import AnalyzeRequest, AnalyzeResponse, DQRuleSet

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analyze", tags=["AI Analysis"])

# In-memory storage for suggested rulesets
_rulesets: Dict[str, DQRuleSet] = {}


@router.post("/suggest-rules")
async def suggest_rules(request: AnalyzeRequest):
    """
    Use Gemini AI to analyze the dataset and suggest DQ rules.

    The AI examines:
    - Column types and statistics
    - Null patterns
    - Value distributions
    - Potential business logic relationships

    Returns suggested rules in structured JSON format.
    """
    try:
        metadata = data_loader.get_metadata(request.dataset_id)
    except KeyError:
        raise HTTPException(
            404, f"Dataset '{request.dataset_id}' not found. Upload first.")

    try:
        result = rule_suggester.suggest_rules(
            dataset_id=request.dataset_id,
            metadata=metadata,
            context=request.context or "",
            max_rules=request.max_rules,
        )

        ruleset: DQRuleSet = result["ruleset"]

        # Store for later use
        _rulesets[request.dataset_id] = ruleset

        return {
            "dataset_id": request.dataset_id,
            "total_rules_suggested": len(ruleset.rules),
            "generated_by": ruleset.generated_by,
            "rules": [rule.model_dump() for rule in ruleset.rules],
            "ai_reasoning": result.get("reasoning", ""),
            "business_observations": result.get("observations", []),
            "data_profile_summary": {
                "row_count": metadata["row_count"],
                "column_count": metadata["column_count"],
                "columns": metadata["columns"],
            }
        }

    except Exception as e:
        logger.error(f"Rule suggestion failed: {e}", exc_info=True)
        raise HTTPException(500, f"AI analysis failed: {str(e)}")


@router.get("/rules/{dataset_id}")
async def get_suggested_rules(dataset_id: str):
    """Get previously suggested rules for a dataset"""
    if dataset_id not in _rulesets:
        raise HTTPException(
            404,
            f"No rules found for dataset '{dataset_id}'. "
            "Call POST /analyze/suggest-rules first."
        )

    ruleset = _rulesets[dataset_id]
    return {
        "dataset_id": dataset_id,
        "total_rules": len(ruleset.rules),
        "generated_by": ruleset.generated_by,
        "generated_at": ruleset.generated_at.isoformat(),
        "rules": [rule.model_dump() for rule in ruleset.rules],
    }


def get_stored_ruleset(dataset_id: str) -> DQRuleSet:
    """Helper to get stored ruleset (used by validation routes)"""
    if dataset_id not in _rulesets:
        raise KeyError(f"No ruleset for '{dataset_id}'")
    return _rulesets[dataset_id]


def store_ruleset(dataset_id: str, ruleset: DQRuleSet):
    """Helper to store a ruleset"""
    _rulesets[dataset_id] = ruleset
