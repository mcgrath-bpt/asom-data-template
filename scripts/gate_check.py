"""
Gate Check Script -- validates evidence completeness for promotion gates.

Demonstrates ASOM v2 gate logic:
- Reads evidence/ledger.jsonl
- Checks all applicable controls have passing evidence
- Detects C-11 emergency overrides and verifies remediation status
- Reports pass/fail with blocking reasons
- Does NOT approve -- only reports readiness

Usage:
    python scripts/gate_check.py --gate G3 --controls C-04,C-05,C-06
    python scripts/gate_check.py --gate G3 --controls C-04,C-05,C-06 --check-overrides

Reference: docs/ASOM_CONTROLS.md (Section 4: Promotion Gates, C-11: Emergency Override)

Key principle: "A gate does not recommend. A gate allows or blocks."
"""
import json
import argparse
import sys
from pathlib import Path
from collections import defaultdict


LEDGER_PATH = Path("evidence/ledger.jsonl")


def load_ledger(path: Path) -> list[dict]:
    """Load all evidence entries from the JSONL ledger."""
    if not path.exists():
        return []
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def check_gate(
    entries: list[dict],
    required_controls: list[str],
    gate_id: str,
) -> dict:
    """Validate evidence completeness against required controls.

    Returns a structured result. Does NOT approve --
    human approval is required separately.
    """
    covered = defaultdict(list)
    for entry in entries:
        if entry.get("status") == "pass":
            covered[entry["control_id"]].append(entry["evidence_id"])

    results = {}
    all_pass = True
    for ctrl in required_controls:
        if ctrl in covered:
            results[ctrl] = {"status": "SATISFIED", "evidence": covered[ctrl]}
        else:
            results[ctrl] = {"status": "MISSING", "evidence": []}
            all_pass = False

    return {
        "gate": gate_id,
        "gate_result": "READY" if all_pass else "BLOCKED",
        "controls": results,
        "note": (
            "All required controls have passing evidence. "
            "Human approval required for promotion."
            if all_pass
            else "Missing evidence blocks promotion. "
            "Remediate and re-run."
        ),
    }


def check_overrides(entries: list[dict]) -> dict:
    """Check for C-11 emergency overrides and their remediation status.

    Returns override summary. Does NOT approve --
    only reports override status for human review.
    """
    overrides = [e for e in entries if e.get("control_id") == "C-11"]
    if not overrides:
        return {"overrides_found": 0, "status": "CLEAN", "note": "No emergency overrides recorded."}

    results = []
    for ov in overrides:
        remediation = ov.get("remediation_status", "unknown")
        results.append({
            "override_id": ov.get("evidence_id"),
            "deferred_controls": ov.get("standard_controls_deferred", []),
            "remediation_deadline": ov.get("remediation_deadline"),
            "remediation_status": remediation,
            "emergency_approver": ov.get("emergency_approver", "unknown"),
        })

    all_remediated = all(r["remediation_status"] == "completed" for r in results)
    any_escalated = any(r["remediation_status"] == "escalated" for r in results)

    return {
        "overrides_found": len(overrides),
        "status": "REMEDIATED" if all_remediated else "ESCALATED" if any_escalated else "PENDING",
        "overrides": results,
        "note": (
            "All overrides remediated. Evidence deferred and subsequently produced."
            if all_remediated
            else "Override remediation incomplete. Review required."
        ),
    }


def main():
    parser = argparse.ArgumentParser(
        description="ASOM Gate Check -- evidence completeness validator"
    )
    parser.add_argument(
        "--gate", required=True, help="Gate ID (G1, G2, G3, G4)"
    )
    parser.add_argument(
        "--controls", required=True, help="Comma-separated control IDs"
    )
    parser.add_argument(
        "--ledger", default=str(LEDGER_PATH), help="Path to ledger JSONL"
    )
    parser.add_argument(
        "--check-overrides",
        action="store_true",
        help="Check for C-11 emergency overrides and remediation status",
    )
    args = parser.parse_args()

    required = [c.strip() for c in args.controls.split(",")]
    entries = load_ledger(Path(args.ledger))
    result = check_gate(entries, required, args.gate)

    if args.check_overrides:
        override_result = check_overrides(entries)
        result["overrides"] = override_result

    print(json.dumps(result, indent=2))
    sys.exit(0 if result["gate_result"] == "READY" else 1)


if __name__ == "__main__":
    main()
