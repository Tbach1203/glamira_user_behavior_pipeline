import pandas as pd

df = pd.read_json("data/raw/product_info.json")

print("Total rows:", len(df))
print("Unique product_id:", df["product_id"].nunique())