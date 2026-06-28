"""Generate realistic sample retail data (orders, customers, products).

Defaults generate 1M+ rows so the lakehouse exercises real Spark/Delta scale.
Use the flags to lower volume for a quick local smoke run, e.g.:
    python data/generate_sample_data.py --orders 20000 --customers 1000
"""

from __future__ import annotations
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
RAW = Path(__file__).resolve().parent / "raw"
RAW.mkdir(parents=True, exist_ok=True)

REGIONS = ["North", "South", "East", "West"]
CATEGORIES = ["Apparel", "Footwear", "Accessories", "Equipment"]


def gen_customers(n_customers: int):
    with open(RAW / "customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "email", "region", "signup_date"])
        for cid in range(1, n_customers + 1):
            signup = fake.date_between(start_date="-3y", end_date="today")
            w.writerow([cid, fake.name(), fake.email(), random.choice(REGIONS), signup])
    print(f"\u2705 customers.csv  ({n_customers:,} rows)")


def gen_products(n_products: int):
    with open(RAW / "products.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "base_price"])
        for pid in range(1, n_products + 1):
            w.writerow([pid, fake.word().title(), random.choice(CATEGORIES), round(random.uniform(10, 500), 2)])
    print(f"\u2705 products.csv   ({n_products:,} rows)")


def gen_orders(n_orders: int, n_customers: int, n_products: int):
    start = datetime.now() - timedelta(days=180)
    with open(RAW / "orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts"])
        for oid in range(1, n_orders + 1):
            ts = start + timedelta(minutes=random.randint(0, 180 * 24 * 60))
            w.writerow([
                oid,
                random.randint(1, n_customers),
                random.randint(1, n_products),
                random.randint(1, 5),
                round(random.uniform(10, 500), 2),
                ts.strftime("%Y-%m-%d %H:%M:%S"),
            ])
            if oid % 100_000 == 0:
                print(f"  ... {oid:,} orders generated", end="\r")
    print(f"\u2705 orders.csv     ({n_orders:,} rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate sample retail data for the lakehouse.")
    ap.add_argument("--orders", type=int, default=1_000_000, help="number of order rows (default 1,000,000)")
    ap.add_argument("--customers", type=int, default=50_000, help="number of customers (default 50,000)")
    ap.add_argument("--products", type=int, default=2_000, help="number of products (default 2,000)")
    args = ap.parse_args()

    gen_customers(args.customers)
    gen_products(args.products)
    gen_orders(args.orders, args.customers, args.products)
    total = args.orders + args.customers + args.products
    print(f"\n\U0001F389 Sample data generated in data/raw/  (~{total:,} total records)")
