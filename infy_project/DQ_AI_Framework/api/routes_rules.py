import logging
from typing import List

from fastapi import APIRouter, HTTPException

from models.schemas import DQRule, DQRuleSet
from api.routes_analysis import get_stored_ruleset, store_ruleset

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/rules", tags=["Rule Management"])


@router.get("/{dataset_id}")
async def get_rules(dataset_id: str):
    """Get current rules for a dataset"""
    try:
        ruleset = get_stored_ruleset(dataset_id)
        return {
            "dataset_id": dataset_id,
            "total_rules": len(ruleset.rules),
            "generated_by": ruleset.generated_by,
            "rules": [r.model_dump() for r in ruleset.rules],
        }
    except KeyError:
        raise HTTPException(404, f"No rules for dataset '{dataset_id}'")


@router.post("/{dataset_id}")
async def set_rules(dataset_id: str, rules: List[DQRule]):
    """Manually set/override rules for a dataset"""
    ruleset = DQRuleSet(
        dataset_id=dataset_id,
        rules=rules,
        generated_by="manual",
    )
    store_ruleset(dataset_id, ruleset)
    return {
        "dataset_id": dataset_id,
        "total_rules": len(rules),
        "status": "rules_saved",
    }


@router.put("/{dataset_id}/toggle/{rule_id}")
async def toggle_rule(dataset_id: str, rule_id: str, active: bool):
    """Enable/disable a specific rule"""
    try:
        ruleset = get_stored_ruleset(dataset_id)
        for rule in ruleset.rules:
            if rule.rule_id == rule_id:
                rule.active = active
                store_ruleset(dataset_id, ruleset)
                return {"rule_id": rule_id, "active": active}
        raise HTTPException(404, f"Rule '{rule_id}' not found")
    except KeyError:
        raise HTTPException(404, f"No rules for dataset '{dataset_id}'")


@router.delete("/{dataset_id}/{rule_id}")
async def delete_rule(dataset_id: str, rule_id: str):
    """Delete a specific rule"""
    try:
        ruleset = get_stored_ruleset(dataset_id)
        original_count = len(ruleset.rules)
        ruleset.rules = [r for r in ruleset.rules if r.rule_id != rule_id]
        if len(ruleset.rules) == original_count:
            raise HTTPException(404, f"Rule '{rule_id}' not found")
        store_ruleset(dataset_id, ruleset)
        return {"rule_id": rule_id, "status": "deleted"}
    except KeyError:
        raise HTTPException(404, f"No rules for dataset '{dataset_id}'")
