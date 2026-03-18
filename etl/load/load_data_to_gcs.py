import os
import logging
from google.cloud import storage
from datetime import datetime

logging.basicConfig(level=logging.INFO)

BUCKET_NAME = os.getenv("GCS_BUCKET")

def get_gcs_client():
    return storage.Client()

def upload_file_to_gcs(local_path, bucket_name, destination_blob):
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_path)
        logging.info(f"Uploaded {local_path} → gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        logging.error(f"Upload failed for {local_path}: {e}")
        raise

def export_to_gcs(ip_path, product_path):
    logging.info("Start exporting to GCS")
    today = datetime.now().strftime("%Y-%m-%d")
    # Define GCS paths (structured)
    ip_gcs_path = f"processed/ip_locations/dt={today}/ip_locations.csv"
    product_gcs_path = f"processed/products/dt={today}/product_info.jsonl"
    upload_file_to_gcs(ip_path, BUCKET_NAME, ip_gcs_path)
    upload_file_to_gcs(product_path, BUCKET_NAME, product_gcs_path)
    logging.info("Export to GCS completed")