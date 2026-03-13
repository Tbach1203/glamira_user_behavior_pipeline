import argparse
from src.data.process_ip_locations import process_ip_locations
from etl.extract.extract_urls import product_map, save_urls
from src.data.product_name_collection import collect_product
from config.connect import connect

def parse_args():
    parser = argparse.ArgumentParser(
        description="Argparse for Project"
    )

    parser.add_argument(
        "--bin-file",
        default="IP-COUNTRY-REGION-CITY.BIN",
        #required=True,
        help="Path to IP2Location BIN file"
    )
    parser.add_argument(
        "--output-location-csv",
        default="data/raw/ip_locations.csv",
        help="Path to output CSV location file"
    )
    parser.add_argument(
        "--output-product-info",
        default="data/raw/product_info.json",
        help="Path to output CSV location file"
    )
    parser.add_argument(
        "--urls-json",
        default="data/raw/urls.json",
        help="Path to output Json urls file"
    )
    parser.add_argument(
        "--error-product-id",
        default="data/raw/error_id.txt",
        help="Path to output Json urls file"
    )
    return parser.parse_args()

if __name__ == "__main__":
    arg = parse_args()
    client = connect()
    db = client["countly"]
    #process_ip_locations(arg.bin_file, arg.output_location_csv, db)
    products = product_map(db)
    save_urls(products, arg.urls_json)
    #collect_product(arg.urls_json,arg.output_product_info, arg.error_product_id)

    