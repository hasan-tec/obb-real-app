"""
diag_sheena_confirmed.py — one-off diagnostic (READ ONLY, no writes).

Verifies Sheena's 2026-07-30 2:26pm Slack answers against LIVE VeraCore
inventory before anything gets written to the items table:
  Baby Mama Tank (M)      -> OBB-BABYMAMATANK(M)
  Mother Noun Tshirt (M)  -> OBB-MOTHERNOUNMATERNITYTSHIRT(M)
  Hanatonic Nauseareliefshot -> OBB-HANATONIC+NAUSEARELIEFSHOT
  Obb - Mama Cap          -> OBB-MAMAHAT(BLACK)
"""
import os
import sys

os.environ["OBB_DISABLE_SCHEDULER"] = "1"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import get_veracore_client  # noqa: E402

ANSWERS = {
    "Baby Mama Tank (Medium)": "OBB-BABYMAMATANK(M)",
    "Mother Noun Maternity Tshirt (Medium)": "OBB-MOTHERNOUNMATERNITYTSHIRT(M)",
    "Hanatonic Nauseareliefshot": "OBB-HANATONIC+NAUSEARELIEFSHOT",
    "Obb - Mama Cap": "OBB-MAMAHAT(BLACK)",
}

vc = get_veracore_client()
inv = vc.get_inventory()
vc_by_stripped_upper = {r["sku"].strip().upper(): r["sku"] for r in inv}

for name, answer in ANSWERS.items():
    exact = vc_by_stripped_upper.get(answer.strip().upper())
    print(f"{name}: answer={answer!r}")
    if exact:
        print(f"   -> EXACT live match: {exact!r}  [CONFIRMED]")
    else:
        print(f"   -> NOT FOUND in live inventory ({len(inv)} rows)")
    print()

print("--- raw live rows containing 'mothernoun' ---")
for r in inv:
    if "mothernoun" in r["sku"].lower():
        print(f"   {r['sku']!r}")

print("\n--- raw live rows containing 'hanatonic' ---")
for r in inv:
    if "hanatonic" in r["sku"].lower():
        print(f"   {r['sku']!r}")

print("\n--- raw live rows containing 'mamahat' ---")
for r in inv:
    if "mamahat" in r["sku"].lower().replace(" ", ""):
        print(f"   {r['sku']!r}")
