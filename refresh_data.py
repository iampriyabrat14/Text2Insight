"""
refresh_data.py — Force-reseed DuckDB with fresh data and export all tables as CSV.

Usage:
    python refresh_data.py

Outputs CSV files to: data/csv_export/
"""
import logging
import random
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DUCKDB_PATH   = "./data/sales.duckdb"
CSV_OUTPUT    = Path("./data/csv_export")
SEED          = 42

random.seed(SEED)
np.random.seed(SEED)

# ── Lookup data ───────────────────────────────────────────────────────────────
REGIONS = ["North", "South", "East", "West", "Central"]
SEGMENTS = ["Enterprise", "SMB", "Startup", "Government"]
CHANNELS = ["Online", "Direct", "Partner", "Retail"]
ORDER_STATUSES = ["Completed", "Pending", "Cancelled", "Refunded"]
PRODUCT_CATEGORIES = {
    "Electronics":    ["Laptops", "Monitors", "Peripherals", "Accessories"],
    "Software":       ["Licenses", "Subscriptions", "Support"],
    "Furniture":      ["Desks", "Chairs", "Storage"],
    "Office Supplies":["Stationery", "Paper", "Printers"],
    "Networking":     ["Routers", "Switches", "Cables"],
}
TEAMS = ["Alpha", "Beta", "Gamma", "Delta", "Omega"]
FIRST_NAMES = ["Alice","Bob","Carol","David","Eve","Frank","Grace","Henry",
               "Irene","Jack","Karen","Leo","Mia","Noah","Olivia","Paul",
               "Quinn","Rachel","Sam","Tara","Uma","Victor","Wendy","Xander","Yara","Zoe"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Wilson","Moore","Taylor","Anderson","Thomas","Jackson",
               "White","Harris","Martin","Thompson","Young","King"]
ADJECTIVES  = ["Pro","Lite","Plus","Elite","Basic","Advanced","Ultra","Max"]
NOUNS       = ["Series","Edition","Bundle","Pack","Kit","Suite","Set","Collection"]

# ── Generators ────────────────────────────────────────────────────────────────
from datetime import date, timedelta

def rdate(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def rname():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def remail(name, idx):
    p = name.lower().split()
    return f"{p[0]}.{p[1]}{idx}@example.com"

def make_customers(n=500):
    rows = []
    for i in range(1, n+1):
        nm = rname()
        rows.append({"customer_id":i,"name":nm,"email":remail(nm,i),
                     "region":random.choice(REGIONS),"segment":random.choice(SEGMENTS),
                     "created_at":rdate(date(2020,1,1),date(2024,6,30)).isoformat()})
    return pd.DataFrame(rows)

def make_sales_reps(n=50):
    rows = []
    for i in range(1, n+1):
        nm = rname()
        rows.append({"rep_id":i,"name":nm,"region":random.choice(REGIONS),
                     "team":random.choice(TEAMS),
                     "hire_date":rdate(date(2018,1,1),date(2023,12,31)).isoformat()})
    return pd.DataFrame(rows)

def make_products(n=200):
    rows = []
    for i in range(1, n+1):
        cat = random.choice(list(PRODUCT_CATEGORIES.keys()))
        sub = random.choice(PRODUCT_CATEGORIES[cat])
        price = round(random.uniform(10, 2500), 2)
        cost  = round(price * random.uniform(0.35, 0.70), 2)
        rows.append({"product_id":i,"name":f"{sub} {random.choice(ADJECTIVES)} {random.choice(NOUNS)} {i}",
                     "category":cat,"sub_category":sub,"unit_price":price,"cost":cost})
    return pd.DataFrame(rows)

def make_orders(n=2000, customer_ids=None, rep_ids=None):
    rows = []
    for i in range(1, n+1):
        rows.append({"order_id":i,"customer_id":random.choice(customer_ids),
                     "rep_id":random.choice(rep_ids),
                     "order_date":rdate(date(2022,1,1),date(2024,12,31)).isoformat(),
                     "status":random.choices(ORDER_STATUSES,weights=[.70,.15,.10,.05])[0],
                     "channel":random.choice(CHANNELS),"region":random.choice(REGIONS)})
    return pd.DataFrame(rows)

def make_order_items(n=5000, order_ids=None, products_df=None):
    rows = []
    for i in range(1, n+1):
        p   = products_df.sample(1).iloc[0]
        qty = random.randint(1, 20)
        disc= round(random.choices([0,.05,.10,.15,.20],weights=[.40,.25,.20,.10,.05])[0],2)
        rows.append({"item_id":i,"order_id":random.choice(order_ids),
                     "product_id":int(p["product_id"]),"quantity":qty,"discount":disc,
                     "sale_price":round(p["unit_price"]*qty*(1-disc),2)})
    return pd.DataFrame(rows)

def make_targets(rep_ids):
    rows = []; tid = 1
    for year in [2023,2024]:
        for q in [1,2,3,4]:
            for rid in rep_ids:
                rows.append({"target_id":tid,"rep_id":rid,"year":year,"quarter":q,
                             "revenue_target":round(random.uniform(50000,300000),2),
                             "units_target":random.randint(100,1000)})
                tid += 1
    return pd.DataFrame(rows)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 55)
    logger.info("  REFRESH DATA + CSV EXPORT")
    logger.info("=" * 55)

    # Generate DataFrames
    customers   = make_customers(500)
    sales_reps  = make_sales_reps(50)
    products    = make_products(200)
    orders      = make_orders(2000,
                    customer_ids=customers["customer_id"].tolist(),
                    rep_ids=sales_reps["rep_id"].tolist())
    order_items = make_order_items(5000,
                    order_ids=orders["order_id"].tolist(),
                    products_df=products)
    targets     = make_targets(sales_reps["rep_id"].tolist())

    tables = {
        "customers":   customers,
        "sales_reps":  sales_reps,
        "products":    products,
        "orders":      orders,
        "order_items": order_items,
        "targets":     targets,
    }

    # ── Write to DuckDB (force drop + recreate) ───────────────────────────
    logger.info("\n[1/2] Reseeding DuckDB at %s ...", DUCKDB_PATH)
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    for name, df in tables.items():
        conn.execute(f"DROP TABLE IF EXISTS {name}")
        conn.register("_tmp", df)
        conn.execute(f"CREATE TABLE {name} AS SELECT * FROM _tmp")
        conn.unregister("_tmp")
        logger.info("  ✓ %-15s  %d rows", name, len(df))
    conn.close()
    logger.info("DuckDB refreshed.\n")

    # ── Export to CSV ─────────────────────────────────────────────────────
    logger.info("[2/2] Exporting CSV files to %s ...", CSV_OUTPUT)
    CSV_OUTPUT.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        out = CSV_OUTPUT / f"{name}.csv"
        df.to_csv(out, index=False)
        logger.info("  ✓ %-15s  → %s  (%d rows, %d cols)",
                    name, out, len(df), len(df.columns))

    logger.info("\n" + "=" * 55)
    logger.info("  Done! CSV files are in: %s", CSV_OUTPUT.resolve())
    logger.info("=" * 55)
    logger.info("\n  Restart the server for the fresh data to take effect.")
    logger.info("  (Or the running server will use the new DuckDB file automatically)\n")

if __name__ == "__main__":
    main()
