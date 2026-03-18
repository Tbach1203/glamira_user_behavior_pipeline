import os
import json
import logging
import pandas as pd

BATCH_SIZE = 1000

def load_ip_locations(csv_path, db):
    logging.info(f"Loading IP locations from {csv_path}")
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    collection = db["ip_locations"]
    logging.info(f"Inserting {len(records)} records into ip_locations")
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        collection.insert_many(batch, ordered=False)
    logging.info("Insert ip_locations done")

def load_products(jsonl_path, db):
    logging.info(f"Loading products from {jsonl_path}")
    collection = db["products"]
    batch = []
    total = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line.strip())
            if "product_id" not in record:
                continue
            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                batch = []
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)
    logging.info(f"Inserted {total} products")

def export_to_mongodb(location_path, product_path, db):
    logging.info("Start loading to MongoDB")
    load_ip_locations(location_path, db)
    load_products(product_path, db)
    logging.info("All data loaded successfully")