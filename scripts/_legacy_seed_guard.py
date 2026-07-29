"""
_legacy_seed_guard.py
=====================
Shared quarantine guard for the legacy seed/import scripts.

WHY THIS EXISTS
---------------
These scripts create `items` rows from hard-coded lists / CSVs that carry the OLD
item SKUs (the messy pre-VeraCore names). They "insert on miss" — if an item SKU
isn't found, they create it.

Once the VeraCore SKU reconciliation runs (see SHEENA_SKU_APPLY_PLAN.md), duplicate
item rows get merged and the losing rows are DELETED. If any of these scripts is then
re-run, it will not find the old SKUs, and will happily **re-create every duplicate
that was just deleted** — silently undoing the merge and re-breaking the engine's
duplicate detection (two rows for one physical product are invisible to each other).

They are therefore quarantined: they refuse to run unless the operator explicitly
opts in, which forces a conscious decision.

TO RUN ONE ANYWAY
-----------------
    OBB_ALLOW_LEGACY_SEED=1 python scripts/<name>.py --dry-run          (bash)
    $env:OBB_ALLOW_LEGACY_SEED=1; python scripts/<name>.py --dry-run    (PowerShell)

Before doing that, confirm the SKUs in the script still match what's in the DB.
"""
import os
import sys

ENV_FLAG = "OBB_ALLOW_LEGACY_SEED"


def assert_allowed(script_name: str) -> None:
    """Abort unless the operator explicitly opted in via OBB_ALLOW_LEGACY_SEED=1."""
    if os.getenv(ENV_FLAG) == "1":
        print(f"[QUARANTINE] {ENV_FLAG}=1 set — running '{script_name}' anyway. "
              f"Confirm its SKUs still match the DB.", file=sys.stderr)
        return

    print(
        f"\n  REFUSING TO RUN: {script_name}\n"
        f"  {'-' * 66}\n"
        f"  This script creates items from OLD (pre-VeraCore) SKUs and inserts any it\n"
        f"  cannot find. Re-running it after the SKU merge would RE-CREATE the duplicate\n"
        f"  item rows that were deleted, silently breaking duplicate detection.\n\n"
        f"  See SHEENA_SKU_APPLY_PLAN.md (hazard H2).\n\n"
        f"  If you are certain this is safe, opt in explicitly:\n"
        f"      OBB_ALLOW_LEGACY_SEED=1 python scripts/{script_name} --dry-run\n",
        file=sys.stderr,
    )
    sys.exit(2)
