"""
BDA Project - Amazon Reviews 2023 Resumable Dataset Downloader
Source: McAuley-Lab/Amazon-Reviews-2023 (Official UCSD Lab Dataset)
Target Categories:
  1. Appliances.jsonl (~929.5 MB)
  2. Musical_Instruments.jsonl (~1.56 GB)
Total Combined Size: ~2.49 GB (Exceeds the 2.0 GB minimum requirement)
"""

import os
import sys
import time
import requests

DATASET_CONFIG = [
    {
        "category": "Appliances",
        "filename": "Appliances.jsonl",
        "url": "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/Appliances.jsonl",
        "expected_bytes": 929451412
    },
    {
        "category": "Musical_Instruments",
        "filename": "Musical_Instruments.jsonl",
        "url": "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/Musical_Instruments.jsonl",
        "expected_bytes": 1557845107
    }
]

def download_file_resumable(url, destination_path, expected_size, chunk_size=1024*1024):
    """
    Downloads a large file in streaming chunks with HTTP Range resume support.
    Never loads the file into RAM.
    """
    downloaded_bytes = 0
    if os.path.exists(destination_path):
        downloaded_bytes = os.path.getsize(destination_path)
        if downloaded_bytes >= expected_size:
            print(f"  [OK] Already downloaded: {destination_path} ({downloaded_bytes / (1024**3):.2f} GB)")
            return downloaded_bytes

    headers = {}
    if downloaded_bytes > 0:
        headers["Range"] = f"bytes={downloaded_bytes}-"
        print(f"  [RESUME] Resuming download from {downloaded_bytes / (1024**2):.2f} MB...")

    response = requests.get(url, headers=headers, stream=True, timeout=30)
    if response.status_code not in (200, 206):
        raise RuntimeError(f"Download failed with HTTP status {response.status_code}")

    mode = "ab" if downloaded_bytes > 0 else "wb"
    total_size = expected_size
    start_time = time.time()
    last_print = start_time

    with open(destination_path, mode) as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded_bytes += len(chunk)
                now = time.time()
                if now - last_print >= 2.0 or downloaded_bytes >= total_size:
                    elapsed = max(now - start_time, 0.001)
                    speed_mb = (len(chunk) / (1024**2)) / max(now - last_print, 0.001)
                    percent = (downloaded_bytes / total_size) * 100
                    print(f"  [{percent:5.1f}%] {downloaded_bytes / (1024**3):.2f} / {total_size / (1024**3):.2f} GB "
                          f"| Speed: {speed_mb:.2f} MB/s | Elapsed: {elapsed:.0f}s", end="\r")
                    last_print = now

    print(f"\n  [DONE] Successfully downloaded {destination_path} ({downloaded_bytes / (1024**3):.2f} GB)")
    return downloaded_bytes

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_dir = os.path.join(base_dir, "dataset", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    print("=" * 70)
    print("BDA MINI PRACTICE CASE STUDY - DATASET INGESTION")
    print("Dataset: Amazon Reviews 2023 (McAuley Lab, UCSD)")
    print(f"Target Directory: {raw_dir}")
    print("=" * 70)

    total_bytes = 0
    start_all = time.time()

    for item in DATASET_CONFIG:
        print(f"\n--> Ingesting Category: {item['category']}")
        dest_path = os.path.join(raw_dir, item["filename"])
        bytes_got = download_file_resumable(item["url"], dest_path, item["expected_bytes"])
        total_bytes += bytes_got

    total_gb = total_bytes / (1024**3)
    total_mb = total_bytes / (1024**2)
    duration = time.time() - start_all

    print("\n" + "=" * 70)
    print("DATASET INGESTION AUDIT SUMMARY")
    print(f"Total Files Downloaded: {len(DATASET_CONFIG)}")
    print(f"Total Bytes Downloaded: {total_bytes:,} bytes")
    print(f"Total Size in MB:       {total_mb:,.2f} MB")
    print(f"Total Size in GB:       {total_gb:,.3f} GB")
    print(f"Target Requirement:     >= 2.000 GB")
    if total_gb >= 2.0:
        print("STATUS: VERIFIED (Strictly exceeds 2.0 GB requirement)")
    else:
        print("STATUS: FAILED (Under 2.0 GB)")
    print(f"Total Elapsed Time:     {duration:.1f} seconds")
    print("=" * 70)

if __name__ == "__main__":
    main()
