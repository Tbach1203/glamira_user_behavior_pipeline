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

def upload_product_info(local_path, destination_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_name)
        FIELDS = [
            "_id", "product_id", "name", "sku", "attribute_set_id",
            "attribute_set", "type_id", "price", "min_price", "max_price",
            "min_price_format", "max_price_format", "gold_weight",
            "none_metal_weight", "fixed_silver_weight", "material_design",
            "qty", "collection", "collection_id", "product_type",
            "product_type_value", "category", "category_name",
            "store_code", "platinum_palladium_info_in_alloy",
            "bracelet_without_chain", "show_popup_quantity_eternity",
            "visible_contents", "gender"
        ]
        with blob.open("wb") as gcs_file:
            with open(local_path, "r", encoding="utf-8") as f:
                for line in f:
                    data = json.loads(line)
                    cleaned = {k: data.get(k) for k in FIELDS if k in data}

                    gcs_file.write((json.dumps(cleaned, ensure_ascii=False) + "\n").encode("utf-8"))

        logging.info(f"Uploaded cleaned data → gs://{BUCKET_NAME}/{destination_name}")
    except Exception as e:
        logging.error(f"Upload failed: {e}")
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
    upload_product_info(product_path, f"product_info.jsonl")
    logging.info("Exporting MongoDB collection: summary")
    collection = db["summary"]
    total = mongo_to_jsonl(collection, f"glamira_raw.jsonl")
    logging.info(f"Exported {total} records → gs://{BUCKET_NAME}/glamira_raw.jsonl")
    logging.info("Export pipeline completed successfully")