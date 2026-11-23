"""Daily order aggregation with a repeatable SQLite load."""
import csv
import sqlite3
from pathlib import Path
from decimal import Decimal
from collections import defaultdict

import pendulum
from airflow.decorators import dag, task

ROOT = Path(__file__).resolve().parents[1]


@dag(schedule="@daily", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
     catchup=False, max_active_runs=1)
def orders_etl():
    @task(retries=2)
    def extract():
        with (ROOT / "data/orders.csv").open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))

    @task
    def transform(records):
        totals = defaultdict(int)
        seen = set()
        for row in records:
            order_id = row["order_id"].strip()
            region = row["region"].strip().lower()
            cents = Decimal(row["amount"]) * 100
            if (not order_id or order_id in seen or not region or not cents.is_finite()
                    or cents < 0 or cents != cents.to_integral_value()):
                raise ValueError(f"Invalid order {order_id!r}")
            seen.add(order_id)
            totals[region] += int(cents)
        if not totals:
            raise ValueError("Input contains no orders")
        return dict(totals)

    @task(retries=2)
    def load(totals):
        output = ROOT / "output"
        output.mkdir(exist_ok=True)
        with sqlite3.connect(output / "sales.db") as db:
            db.execute("CREATE TABLE IF NOT EXISTS regional_sales "
                       "(region TEXT PRIMARY KEY, revenue_cents INTEGER NOT NULL)")
            db.execute("DELETE FROM regional_sales")
            db.executemany("INSERT INTO regional_sales VALUES (?, ?)", sorted(totals.items()))

    load(transform(extract()))


orders_etl()
