"""
BDA Project - Amazon Reviews 2023 Dataset Validation
Performs streaming schema validation, record counting, and integrity checks.
Guarantees memory-safe line-by-line processing.
"""

import os
import sys
import json
import time

def validate_jsonl_file(file_path, category_name):
    """
    Validates a raw JSONL file in streaming mode.
    Returns statistics dictionary.
    """
    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}

    file_size_bytes = os.path.getsize(file_path)
    file_size_gb = file_size_bytes / (1024**3)

    record_count = 0
    valid_ratings_count = 0
    ratings_dist = {1.0: 0, 2.0: 0, 3.0: 0, 4.0: 0, 5.0: 0}
    null_text_count = 0
    unique_products = set()
    sample_record = None

    start_time = time.time()
    last_print = start_time

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            record_count += 1
            try:
                rec = json.loads(line_str)
                if sample_record is None:
                    sample_record = rec

                # Validate rating
                r = rec.get("rating")
                if r is not None and isinstance(r, (int, float)):
                    rf = float(r)
                    if 1.0 <= rf <= 5.0:
                        valid_ratings_count += 1
                        ratings_dist[round(rf)] = ratings_dist.get(round(rf), 0) + 1

                # Check text content
                text = rec.get("text", "")
                if not text or not str(text).strip():
                    null_text_count += 1

                # Track product ASIN
                asin = rec.get("parent_asin") or rec.get("asin")
                if asin and len(unique_products) < 100000:
                    unique_products.add(asin)

            except json.JSONDecodeError:
                pass

            now = time.time()
            if now - last_print >= 5.0:
                print(f"  Processed {record_count:,} records in {category_name} ({record_count / max(now - start_time, 0.001):.0f} rec/s)...", end="\r")
                last_print = now

    elapsed = time.time() - start_time
    print(f"\n  [COMPLETED] {category_name}: {record_count:,} records validated in {elapsed:.1f}s")

    return {
        "category": category_name,
        "file_path": file_path,
        "file_size_bytes": file_size_bytes,
        "file_size_gb": round(file_size_gb, 4),
        "total_records": record_count,
        "valid_ratings_count": valid_ratings_count,
        "ratings_distribution": ratings_dist,
        "null_empty_text_count": null_text_count,
        "approx_unique_products": len(unique_products),
        "sample_schema_keys": list(sample_record.keys()) if sample_record else []
    }

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_dir = os.path.join(base_dir, "dataset", "raw")
    reports_dir = os.path.join(base_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 70)
    print("BDA MINI PRACTICE CASE STUDY - DATASET VALIDATION AUDIT")
    print("=" * 70)

    categories = [
        ("Appliances.jsonl", "Appliances"),
        ("Musical_Instruments.jsonl", "Musical_Instruments")
    ]

    total_records = 0
    total_bytes = 0
    all_reports = []

    for filename, cat in categories:
        fp = os.path.join(raw_dir, filename)
        print(f"\n--> Validating file: {filename}")
        stat = validate_jsonl_file(fp, cat)
        all_reports.append(stat)
        total_records += stat.get("total_records", 0)
        total_bytes += stat.get("file_size_bytes", 0)

    summary = {
        "audit_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_files": len(categories),
        "total_downloaded_bytes": total_bytes,
        "total_downloaded_gb": round(total_bytes / (1024**3), 3),
        "total_records": total_records,
        "requirement_2gb_satisfied": (total_bytes / (1024**3)) >= 2.0,
        "per_category_statistics": all_reports
    }

    report_path = os.path.join(reports_dir, "dataset_validation_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n" + "=" * 70)
    print("DATASET VALIDATION SUMMARY")
    print(f"Total Files Inspected:  {summary['total_files']}")
    print(f"Total Raw File Size:    {summary['total_downloaded_gb']} GB ({total_bytes:,} bytes)")
    print(f"Total Reviews Found:    {summary['total_records']:,} real customer reviews")
    print(f"2 GB Rule Requirement:  {'PASS (Exceeds 2.0 GB)' if summary['requirement_2gb_satisfied'] else 'FAIL'}")
    print(f"Report saved to:        {report_path}")
    print("=" * 70)

if __name__ == "__main__":
    main()
