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

def clean_string(value):
    if value is None:
        return None
    s = str(value)
    s = s.replace("\n", " ").replace("\r", " ")
    s = "".join(ch for ch in s if ord(ch) >= 32)
    return s.strip()

def normalize_data(data):
    return {
        k: clean_string(v)
        for k, v in data.items()
    }

def upload_product_info(local_path, destination_name):
    try:
        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_name)

        base_dir = os.path.dirname(local_path)
        file_name = os.path.basename(local_path)
        name, ext = os.path.splitext(file_name)
        local_output_path = os.path.join(base_dir, f"{name}_clean{ext}")
        FIELDS = [
            "product_id", "name", "sku", "attribute_set_id",
            "attribute_set", "type_id", "price", "min_price", "max_price",
            "min_price_format", "max_price_format", "gold_weight",
            "none_metal_weight", "fixed_silver_weight", "material_design",
            "qty", "collection", "collection_id", "product_type",
            "product_type_value", "category", "category_name",
            "store_code", "platinum_palladium_info_in_alloy",
            "bracelet_without_chain", "show_popup_quantity_eternity",
            "visible_contents", "gender"
        ]
        total = 0
        with blob.open("wb") as gcs_file, \
             open(local_output_path, "w", encoding="utf-8") as local_file, \
             open(local_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except Exception as e:
                    logging.warning(f"JSON lỗi dòng {i}: {e}")
                    continue
                cleaned = {k: data.get(k) for k in FIELDS if k in data}
                cleaned = normalize_data(cleaned)
                line_out = json.dumps(cleaned, ensure_ascii=False) + "\n"
                gcs_file.write(line_out.encode("utf-8"))
                local_file.write(line_out)
                total += 1
        logging.info(f"Uploaded {total} records → gs://{BUCKET_NAME}/{destination_name}")
        logging.info(f"Saved clean file → {local_output_path}")
    except Exception as e:
        logging.error(f"Upload failed: {e}")
        raise

def normalize_option(option):
    if option is None:
        return []
    if isinstance(option, dict):
        return [option]
    if isinstance(option, list):
        return option
    return []

def normalize_cart_products(cart_products):
    if cart_products is None:
        return []
    if isinstance(cart_products, dict):
        cart_products = [cart_products]
    normalized = []
    for cp in cart_products:
        if not isinstance(cp, dict):
            continue

        cp["option"] = normalize_option(cp.get("option"))
        normalized.append(cp)
    return normalized

def clean_doc(doc):
    for k, v in doc.items():
        if isinstance(v, str):
            doc[k] = clean_string(v)
    return doc


def normalize_record(doc):
    if "_id" in doc:
        del doc["_id"]
    doc["cart_products"] = normalize_cart_products(doc.get("cart_products"))
    doc["option"] = normalize_option(doc.get("option"))
    doc = clean_doc(doc)
    return doc

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
                try:
                    doc = normalize_record(doc)

                    line = json.dumps(doc, ensure_ascii=False) + "\n"
                    f.write(line.encode("utf-8"))
                    total += 1

                except Exception as e:
                    logging.warning(f"Error normalize doc: {e}")
                    continue
        cursor.close()
        logging.info(f"Exported {total} records → gs://{BUCKET_NAME}/{blob_name}")
        return total
    except Exception as e:
        logging.error(f"Mongo → JSONL failed: {e}")
        raise

def export_to_gcs(ip_path, product_path, db):
    logging.info("Start exporting data to GCS")
    upload_file_to_gcs(ip_path, "ip_locations.jsonl")
    upload_product_info(product_path, "product_info.jsonl")
    logging.info("Exporting MongoDB collection: summary")
    collection = db["summary"]
    total = mongo_to_jsonl(collection, "glamira_raw.jsonl")
    logging.info(f"Exported {total} records → gs://{BUCKET_NAME}/glamira_raw.jsonl")
    logging.info("Export pipeline completed successfully")