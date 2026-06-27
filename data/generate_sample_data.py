"""Generate realistic sample retail data (orders, customers, products)."""

from __future__ import annotations
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
RAW = Path(__file__).resolve().parent / "raw"
RAW.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 1_000
N_PRODUCTS = 200
N_ORDERS = 20_000
REGIONS = ["North", "South", "East", "West"]


def gen_customers():
    with open(RAW / "customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "email", "region", "signup_date"])
        for cid in range(1, N_CUSTOMERS + 1):
            signup = fake.date_between(start_date="-3y", end_date="today")
            w.writerow([cid, fake.name(), fake.email(), random.choice(REGIONS), signup])
    print(f"✅ customers.csv  ({N_CUSTOMERS:,} rows)")


def gen_products():
    with open(RAW / "products.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "base_price"])
        cats = ["Apparel", "Footwear", "Accessories", "Equipment"]
        for pid in range(1, N_PRODUCTS + 1):
            w.writerow([pid, fake.word().title(), random.choice(cats), round(random.uniform(10, 500), 2)])
    print(f"✅ products.csv   ({N_PRODUCTS:,} rows)")


def gen_orders():
    start = datetime.now() - timedelta(days=180)
    with open(RAW / "orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts"])
        for oid in range(1, N_ORDERS + 1):
            ts = start + timedelta(minutes=random.randint(0, 180 * 24 * 60))
            w.writerow([
                oid,
                random.randint(1, N_CUSTOMERS),
                random.randint(1, N_PRODUCTS),
                random.randint(1, 5),
                round(random.uniform(10, 500), 2),
                ts.strftime("%Y-%m-%d %H:%M:%S"),
            ])
    print(f"✅ orders.csv     ({N_ORDERS:,} rows)")


if __name__ == "__main__":
    gen_customers()
    gen_products()
    gen_orders()
    print("\n🎉 Sample data generated in data/raw/")
