import hashlib

unique_hashes = set()

with open("data/raw/product_info.jsonl", "r", encoding="utf-8") as f:
    for i, line in enumerate(f, 1):
        h = hashlib.md5(line.strip().encode()).hexdigest()
        unique_hashes.add(h)

        if i % 100000 == 0:
            print("Processed:", i)

print("Unique rows:", len(unique_hashes))