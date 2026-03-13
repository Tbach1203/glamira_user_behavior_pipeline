import logging
import json

logging.basicConfig(level=logging.INFO)

EVENTS = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

SPECIAL_EVENTS = "product_view_all_recommend_clicked"
MAX_URL_PER_PRODUCT = 10

def product_map(db):
    logging.info("Start extracting product urls")
    collection = db["summary"]
    pipeline = [
        {
            "$match": {
                "collection": {
                    "$in": EVENTS + [SPECIAL_EVENTS]
                }
            }
        },
        {
            "$project": {
                "product_id": {
                    "$ifNull": ["$product_id", "$viewing_product_id"]
                },
                "url": {
                    "$ifNull": ["$current_url", "$referrer_url"]
                }
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None},
                "url": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "product_id": "$product_id",
                    "url": "$url"
                }
            }
        }
    ]
    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=10000
    )
    products = {}
    for doc in cursor:
        pid = doc["_id"]["product_id"]
        url = doc["_id"]["url"]
        if pid not in products:
            products[pid] = []
        if len(products[pid]) < MAX_URL_PER_PRODUCT:
            products[pid].append(url)
    logging.info(f"Collected {len(products)} unique product_ids")
    return products

def save_urls(products, output_path):
    logging.info(f"Saving results to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    logging.info("Saved JSON successfully")