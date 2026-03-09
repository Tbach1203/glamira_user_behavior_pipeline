import argparse
from config.connect import connect
from src.data.process_ip_locations import process_ip_locations

def parse_args():
    parser = argparse.ArgumentParser(
        description="Argparse for Project"
    )

    parser.add_argument(
        "--bin-file",
        required=True,
        help="Path to IP2Location BIN file"
    )

    parser.add_argument(
        "--output-location-csv",
        default="data/processed/ip_locations.csv",
        help="Path to output CSV location file"
    )
    return parser.parse_args()

if __name__ == "__main__":
    arg = parse_args()
    process_ip_locations(arg.bin_file, arg.output_location_csv)
