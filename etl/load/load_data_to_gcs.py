import os
import json
import logging
from google.cloud import storage
from datetime import datetime

logging.basicConfig(level=logging.INFO)

BUCKET_NAME = os.getenv("GCS_BUCKET")

def get_gcs_client():
    return storage.Client()

def upload_file_to_gcs(local_path, destination_blob):
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_path)
        logging.info(f"Uploaded {local_path} → gs://{BUCKET_NAME}/{destination_blob}")
    except Exception as e:
        logging.error(f"Upload failed for {local_path}: {e}")
        raise

def mongo_to_jsonl(collection, blob):
    total = 0
    try:
        if blob.exists():
            logging.info(f"Skip Mongo export (already exists): {blob.name}")
            return 0
        cursor = collection.find({}, no_cursor_timeout=True)
        with blob.open("wb") as f:
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                line = json.dumps(doc, ensure_ascii=False) + "\n"
                f.write(line.encode("utf-8"))
                total += 1
        cursor.close()
        return total
    except Exception as e:
        logging.error(f"Mongo → JSONL failed: {e}")
        raise

def export_to_gcs(ip_path, product_path, db):
    logging.info("Start exporting raw data to GCS")
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    today = datetime.now().strftime("%Y-%m-%d")
    upload_file_to_gcs(ip_path,f"processed/ip_locations/dt={today}/ip_locations.jsonl")
    upload_file_to_gcs(product_path,f"processed/products/dt={today}/product_info.jsonl")
    logging.info("Exporting MongoDB collection: summary")
    blob = bucket.blob(f"raw/glamira_raw.jsonl")
    collection = db["summary"]
    total = mongo_to_jsonl(collection, blob)
    logging.info(f"Exported {total} records → gs://{BUCKET_NAME}/raw/glamira_raw.jsonl")
    logging.info("Export pipeline completed successfully")