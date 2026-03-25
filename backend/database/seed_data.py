"""
Seed script — generates dummy sales DataFrames and loads them into DuckDB.

Run directly:  python -m backend.database.seed_data
Or call:       seed_all() from app lifespan if DB is empty.

Tables created:
  customers, products, orders, order_items, sales_reps, targets
"""
import logging
import random
import threading
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Reproducible results
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ---------------------------------------------------------------------------
# Constants / lookup data
# ---------------------------------------------------------------------------

REGIONS = ["North", "South", "East", "West", "Central"]
SEGMENTS = ["Enterprise", "SMB", "Startup", "Government"]
CHANNELS = ["Online", "Direct", "Partner", "Retail"]
ORDER_STATUSES = ["Completed", "Pending", "Cancelled", "Refunded"]
PRODUCT_CATEGORIES = {
    "Electronics": ["Laptops", "Monitors", "Peripherals", "Accessories"],
    "Software": ["Licenses", "Subscriptions", "Support"],
    "Furniture": ["Desks", "Chairs", "Storage"],
    "Office Supplies": ["Stationery", "Paper", "Printers"],
    "Networking": ["Routers", "Switches", "Cables"],
}
TEAMS = ["Alpha", "Beta", "Gamma", "Delta", "Omega"]

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry",
    "Irene", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zoe",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson",
    "White", "Harris", "Martin", "Thompson", "Young", "King",
]

PRODUCT_ADJECTIVES = ["Pro", "Lite", "Plus", "Elite", "Basic", "Advanced", "Ultra", "Max"]
PRODUCT_NOUNS = ["Series", "Edition", "Bundle", "Pack", "Kit", "Suite", "Set", "Collection"]


# ---------------------------------------------------------------------------
# Generator functions
# ---------------------------------------------------------------------------

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_email(name: str, idx: int) -> str:
    parts = name.lower().split()
    return f"{parts[0]}.{parts[1]}{idx}@example.com"


def make_customers(n: int = 500) -> pd.DataFrame:
    logger.info("Generating %d customers...", n)
    records = []
    for i in range(1, n + 1):
        name = random_name()
        records.append({
            "customer_id": i,
            "name": name,
            "email": random_email(name, i),
            "region": random.choice(REGIONS),
            "segment": random.choice(SEGMENTS),
            "created_at": random_date(date(2020, 1, 1), date(2024, 6, 30)).isoformat(),
        })
    return pd.DataFrame(records)


def make_sales_reps(n: int = 50) -> pd.DataFrame:
    logger.info("Generating %d sales reps...", n)
    records = []
    for i in range(1, n + 1):
        name = random_name()
        records.append({
            "rep_id": i,
            "name": name,
            "region": random.choice(REGIONS),
            "team": random.choice(TEAMS),
            "hire_date": random_date(date(2018, 1, 1), date(2023, 12, 31)).isoformat(),
        })
    return pd.DataFrame(records)


def make_products(n: int = 200) -> pd.DataFrame:
    logger.info("Generating %d products...", n)
    records = []
    for i in range(1, n + 1):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        sub_category = random.choice(PRODUCT_CATEGORIES[category])
        adj = random.choice(PRODUCT_ADJECTIVES)
        noun = random.choice(PRODUCT_NOUNS)
        unit_price = round(random.uniform(10, 2500), 2)
        cost = round(unit_price * random.uniform(0.35, 0.70), 2)
        records.append({
            "product_id": i,
            "name": f"{sub_category} {adj} {noun} {i}",
            "category": category,
            "sub_category": sub_category,
            "unit_price": unit_price,
            "cost": cost,
        })
    return pd.DataFrame(records)


def make_orders(
    n: int = 2000,
    customer_ids: list[int] = None,
    rep_ids: list[int] = None,
) -> pd.DataFrame:
    logger.info("Generating %d orders...", n)
    records = []
    for i in range(1, n + 1):
        order_date = random_date(date(2022, 1, 1), date(2024, 12, 31))
        records.append({
            "order_id": i,
            "customer_id": random.choice(customer_ids),
            "rep_id": random.choice(rep_ids),
            "order_date": order_date.isoformat(),
            "status": random.choices(
                ORDER_STATUSES, weights=[0.70, 0.15, 0.10, 0.05]
            )[0],
            "channel": random.choice(CHANNELS),
            "region": random.choice(REGIONS),
        })
    return pd.DataFrame(records)


def make_order_items(
    n: int = 5000,
    order_ids: list[int] = None,
    products_df: pd.DataFrame = None,
) -> pd.DataFrame:
    logger.info("Generating %d order items...", n)
    records = []
    for i in range(1, n + 1):
        product = products_df.sample(1).iloc[0]
        quantity = random.randint(1, 20)
        discount = round(random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[0.40, 0.25, 0.20, 0.10, 0.05])[0], 2)
        sale_price = round(product["unit_price"] * quantity * (1 - discount), 2)
        records.append({
            "item_id": i,
            "order_id": random.choice(order_ids),
            "product_id": int(product["product_id"]),
            "quantity": quantity,
            "discount": discount,
            "sale_price": sale_price,
        })
    return pd.DataFrame(records)


def make_targets(rep_ids: list[int]) -> pd.DataFrame:
    logger.info("Generating targets for %d reps across 2 years...", len(rep_ids))
    records = []
    target_id = 1
    for year in [2023, 2024]:
        for quarter in [1, 2, 3, 4]:
            for rep_id in rep_ids:
                records.append({
                    "target_id": target_id,
                    "rep_id": rep_id,
                    "year": year,
                    "quarter": quarter,
                    "revenue_target": round(random.uniform(50_000, 300_000), 2),
                    "units_target": random.randint(100, 1000),
                })
                target_id += 1
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Load into DuckDB
# ---------------------------------------------------------------------------

def _load_all_sync(duckdb_path: str) -> None:
    """Synchronous load — called from executor in async context."""
    path = Path(duckdb_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(path))

    # Check if already seeded
    existing = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='main'"
    ).fetchone()[0]
    if existing >= 6:
        logger.info("DuckDB already seeded (%d tables found), skipping.", existing)
        conn.close()
        return

    logger.info("Seeding DuckDB at %s ...", path)

    customers = make_customers(500)
    sales_reps = make_sales_reps(50)
    products = make_products(200)
    orders = make_orders(
        2000,
        customer_ids=customers["customer_id"].tolist(),
        rep_ids=sales_reps["rep_id"].tolist(),
    )
    order_items = make_order_items(
        5000,
        order_ids=orders["order_id"].tolist(),
        products_df=products,
    )
    targets = make_targets(sales_reps["rep_id"].tolist())

    tables = {
        "customers": customers,
        "sales_reps": sales_reps,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "targets": targets,
    }

    for table_name, df in tables.items():
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.register("_tmp_df", df)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _tmp_df")
        conn.unregister("_tmp_df")
        logger.info("  Loaded %-15s — %d rows", table_name, len(df))

    conn.close()
    logger.info("DuckDB seeding complete.")


_seed_lock = threading.Lock()


def seed_all(duckdb_path: str | None = None) -> None:
    """Synchronous seed entry point (thread-safe)."""
    from backend.config import get_settings
    path = duckdb_path or get_settings().duckdb_path
    with _seed_lock:
        _load_all_sync(path)


async def async_seed_all(duckdb_path: str | None = None) -> None:
    """Async wrapper — runs seed in thread executor so it doesn't block the event loop."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    from backend.config import get_settings

    path = duckdb_path or get_settings().duckdb_path
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        await loop.run_in_executor(pool, _load_all_sync, path)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    # Allow overriding path via CLI arg
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None

    try:
        from backend.config import get_settings
        default_path = get_settings().duckdb_path
    except Exception:
        default_path = "./data/sales.duckdb"

    seed_all(path_arg or default_path)
    print("\nDone. DuckDB file:", path_arg or default_path)
