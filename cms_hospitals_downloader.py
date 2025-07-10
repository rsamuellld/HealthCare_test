# Raymond Samuel  7/11/2025  Health Care testing for Linux. CMS 

#!/usr/bin/env python3
import os
import re
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DOWNLOAD_DIR = "cms_hospitals_data"
METADATA_FILE = "download_metadata.json"
MAX_WORKERS = 5


def to_snake_case(name):
    name = re.sub(r"[’'`]", "", name)
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name.lower()


def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    else:
        return {}


def save_metadata(metadata):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


def download_and_process_dataset(dataset, metadata):
    dataset_id = dataset["id"]
    dataset_title = dataset["title"]
    last_modified = dataset["lastModified"]
    csv_url = dataset.get("distribution", {}).get("downloadURL")

    if not csv_url:
        print(f"No CSV download URL for dataset {dataset_title} ({dataset_id})")
        return None

    last_run_modified = metadata.get(dataset_id)

    if last_run_modified and last_run_modified >= last_modified:
        print(f"Skipping {dataset_title} (not modified since last run)")
        return None

    print(f"Downloading {dataset_title} ...")
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to download {dataset_title}: {e}")
        return None

    try:
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
    except Exception as e:
        print(f"Failed to parse CSV for {dataset_title}: {e}")
        return None

    new_columns = [to_snake_case(col) for col in df.columns]
    df.columns = new_columns

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = f"{dataset_id}_{to_snake_case(dataset_title)}.csv"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    df.to_csv(filepath, index=False)
    print(f"Saved processed data to {filepath}")

    return dataset_id, last_modified


def main():
    print("Loading metadata...")
    metadata = load_metadata()

    print("Fetching CMS datasets metadata...")
    try:
        response = requests.get(METASTORE_URL)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Failed to fetch metastore data: {e}")
        return

    hospital_datasets = [
        ds for ds in data.get("items", [])
        if ds.get("themes") and "hospitals" in ds["themes"].lower()
    ]

    print(f"Found {len(hospital_datasets)} hospital-related datasets.")

    updated_metadata = metadata.copy()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_and_process_dataset, ds, metadata): ds
            for ds in hospital_datasets
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                dataset_id, last_modified = result
                updated_metadata[dataset_id] = last_modified

    save_metadata(updated_metadata)
    print("Download and processing complete.")


if __name__ == "__main__":
    main()
