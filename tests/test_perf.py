"""Quick performance test for forward planner after pagination fix."""
import os, time, logging
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
from projection_engine import project_forward

logging.basicConfig(level=logging.WARNING)

db = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
start = time.time()
result = project_forward(db, base_month="2026-05", ship_day=14, horizon_months=3,
                         warehouse_minimum=100, include_paused=False,
                         lookback_months=4, recency_months=3)
elapsed = time.time() - start
print(f"Elapsed: {elapsed:.1f}s")
print(f"Warnings: {len(result['warnings'])}")
for month_str, mdata in result["months"].items():
    tri = mdata["trimesters"]
    total = sum(t["projected_customers"] for t in tri.values())
    print(f"  {month_str}: T1={tri[1]['projected_customers']}, T2={tri[2]['projected_customers']}, T3={tri[3]['projected_customers']}, T4={tri[4]['projected_customers']} = {total}")
print("DONE")
