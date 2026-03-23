import os
import json
import logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
BUCKET_NAME = os.getenv("GCS_BUCKET")
def get_gcs_client():
    return storage.Client()

def upload_file_to_gcs(local_path, destination_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_name)
        blob.upload_from_filename(local_path)
        logging.info(f"Uploaded {local_path} → gs://{BUCKET_NAME}/{destination_name}")
    except Exception as e:
        logging.error(f"Upload failed for {local_path}: {e}")
        raise

def mongo_to_jsonl(collection, blob_name):
    total = 0
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
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
    logging.info("Start exporting data to GCS")
    upload_file_to_gcs(ip_path, f"ip_locations.jsonl")
    upload_file_to_gcs(product_path, f"product_info.jsonl")
    logging.info("Exporting MongoDB collection: summary")
    collection = db["summary"]
    total = mongo_to_jsonl(collection, f"glamira_raw.jsonl")
    logging.info(f"Exported {total} records → gs://{BUCKET_NAME}/glamira_raw.jsonl")
    logging.info("Export pipeline completed successfully")