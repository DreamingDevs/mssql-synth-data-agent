import os, sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import Config

OUTPUT_RAW_DIR = Config.get_config()["output_raw_dir"]
OUTPUT_DIR = Config.get_config()["output_dir"]
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "consolidated_schema.json")

consolidated = {
    "foreign_keys": [],
    "columns": [],
    "tables": []
}

def detect_file_type(filename: str):
    """Detect file type based on exact filename patterns"""
    fname = filename.lower()
    if fname.endswith("relationships.json"):
        return "foreign_keys"
    elif fname.endswith("schema.json"):
        return "columns"
    elif fname == "tables.json":
        return "tables"
    return None

# Process all JSON files in input folder
for filename in os.listdir(OUTPUT_RAW_DIR):
    if filename.endswith(".json"):
        file_path = os.path.join(OUTPUT_RAW_DIR, filename)
        with open(file_path, "r") as f:
            try:
                data = json.load(f)
                if isinstance(data, dict):  # normalize to list
                    data = [data]
            except json.JSONDecodeError:
                print(f"⚠️ Skipping invalid JSON file: {filename}")
                continue

        ftype = detect_file_type(filename)
        if ftype:
            consolidated[ftype].extend(data)
            print(f"✅ Merged {filename} into {ftype}")
        else:
            print(f"⚠️ Unknown file type for {filename}, skipping.")

# Deduplicate foreign keys
seen = set()
unique_fks = []
for fk in consolidated["foreign_keys"]:
    key = (fk["table"], fk["column"], fk["ref_table"], fk["ref_column"])
    if key not in seen:
        seen.add(key)
        unique_fks.append(fk)
consolidated["foreign_keys"] = unique_fks

# Ensure output dir exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Save consolidated JSON
with open(OUTPUT_FILE, "w") as f:
    json.dump(consolidated, f, indent=2)

print(f"\n💾 Consolidated JSON saved to {OUTPUT_FILE}")
