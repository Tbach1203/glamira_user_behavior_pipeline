import IP2Location
import json
import logging

logging.basicConfig(level=logging.INFO)

def process_ip_locations(bin_file, output_path, db):
    logging.info("Start processing IP locations")
    collection = db["summary"]
    # Aggregate unique IPs
    logging.info("Fetching unique IPs from MongoDB")
    ips_cursor = collection.aggregate([
        {"$group": {"_id": "$ip"}}
    ])
    # Load IP2Location
    logging.info(f"Loading IP2Location BIN: {bin_file}")
    location = IP2Location.IP2Location(bin_file)
    total = 0
    success = 0
    failed = 0
    logging.info(f"Writing output to {output_path} (JSONL format)")
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in ips_cursor:
            total += 1
            ip = doc["_id"]
            try:
                record = location.get_all(ip)

                result = {
                    "ip": ip,
                    "country": record.country_long,
                    "region": record.region,
                    "city": record.city
                }
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                success += 1
            except Exception as e:
                failed += 1
                logging.warning(f"Error processing IP {ip}: {e}")
    logging.info("Processing completed")
    logging.info(f"Total: {total}, Success: {success}, Failed: {failed}")