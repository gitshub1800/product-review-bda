"""
BDA Project - Amazon Reviews 2023 Streaming Data Cleaner & Normalizer
Reads raw JSONL files in streaming mode, performs comprehensive cleaning,
normalizes text, converts timestamps, and saves in Hadoop-compatible TSV format.
"""

import os
import sys
import json
import re
import time
from datetime import datetime

def clean_review_text(text):
    """
    Normalizes review text:
    - Lowercase
    - Removes HTML tags and URLs
    - Removes punctuation and control characters
    - Normalizes whitespace to single spaces
    """
    if not text:
        return ""
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", str(text))
    # Strip URLs
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    # Remove control characters, tabs, newlines
    text = re.sub(r"[\r\n\t]", " ", text)
    # Lowercase
    text = text.lower()
    # Keep alphanumeric and simple whitespace
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Collapse multiple whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def convert_timestamp(ts):
    """Converts epoch milliseconds or seconds to ISO string YYYY-MM-DD."""
    if not ts:
        return "Unknown"
    try:
        ts_num = float(ts)
        if ts_num > 1e11:  # Milliseconds
            ts_num = ts_num / 1000.0
        return datetime.fromtimestamp(ts_num).strftime("%Y-%m-%d")
    except Exception:
        return "Unknown"

def process_file(file_path, category, out_f, seen_signatures, max_records=None):
    """
    Streams through a raw JSONL file, cleans each record, and writes TSV output.
    Uses bounded signature cache to eliminate duplicates without RAM exhaustion.
    """
    total_read = 0
    total_written = 0
    invalid_ratings = 0
    missing_fields = 0
    duplicates = 0
    empty_texts = 0

    start_time = time.time()
    last_print = start_time

    with open(file_path, "r", encoding="utf-8", errors="ignore") as in_f:
        for line in in_f:
            total_read += 1
            if max_records and total_written >= max_records:
                break

            line_str = line.strip()
            if not line_str:
                continue

            try:
                rec = json.loads(line_str)
            except Exception:
                continue

            # 1. Product ID
            asin = rec.get("parent_asin") or rec.get("asin")
            if not asin:
                missing_fields += 1
                continue
            asin = str(asin).strip().replace("\t", "").replace("\n", "")

            # 2. Rating validation
            rating = rec.get("rating")
            if rating is None:
                missing_fields += 1
                continue
            try:
                rating_val = float(rating)
                if rating_val < 1.0 or rating_val > 5.0:
                    invalid_ratings += 1
                    continue
            except (ValueError, TypeError):
                invalid_ratings += 1
                continue

            # 3. User ID & Signature Deduplication
            user_id = str(rec.get("user_id", "")).strip()
            ts_raw = rec.get("timestamp")
            sig = (user_id, asin, str(ts_raw))
            if sig in seen_signatures:
                duplicates += 1
                continue
            if len(seen_signatures) < 2000000:
                seen_signatures.add(sig)

            # 4. Text cleaning
            title = rec.get("title", "")
            body = rec.get("text", "")
            combined_text = f"{title} {body}".strip()
            cleaned_text = clean_review_text(combined_text)
            if not cleaned_text:
                empty_texts += 1
                continue

            # 5. Metadata fields
            date_str = convert_timestamp(ts_raw)
            verified = 1 if rec.get("verified_purchase") is True else 0
            helpful = int(rec.get("helpful_vote") or 0)

            # 6. Sentiment label for ML
            if rating_val <= 2.0:
                sentiment = "negative"
            elif rating_val == 3.0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

            # TSV row format:
            # asin \t rating \t category \t verified \t helpful \t date \t sentiment \t cleaned_text
            out_row = f"{asin}\t{rating_val:.1f}\t{category}\t{verified}\t{helpful}\t{date_str}\t{sentiment}\t{cleaned_text}\n"
            out_f.write(out_row)
            total_written += 1

            now = time.time()
            if now - last_print >= 5.0:
                rec_per_sec = total_read / max(now - start_time, 0.001)
                print(f"  [{category}] Ingested {total_read:,} | Written: {total_written:,} ({rec_per_sec:.0f} rec/s)...", end="\r")
                last_print = now

    elapsed = time.time() - start_time
    print(f"\n  [FINISHED {category}] Read: {total_read:,} | Cleaned & Written: {total_written:,} | Time: {elapsed:.1f}s")

    return {
        "category": category,
        "total_read": total_read,
        "total_written": total_written,
        "invalid_ratings": invalid_ratings,
        "missing_fields": missing_fields,
        "duplicates": duplicates,
        "empty_texts": empty_texts,
        "elapsed_seconds": round(elapsed, 2)
    }

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_dir = os.path.join(base_dir, "dataset", "raw")
    processed_dir = os.path.join(base_dir, "dataset", "processed")
    reports_dir = os.path.join(base_dir, "reports")
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)

    out_file = os.path.join(processed_dir, "cleaned_reviews.tsv")

    print("=" * 70)
    print("BDA MINI PRACTICE CASE STUDY - STREAMING DATA CLEANING")
    print(f"Output Target: {out_file}")
    print("=" * 70)

    files_to_process = [
        (os.path.join(raw_dir, "Appliances.jsonl"), "Appliances"),
        (os.path.join(raw_dir, "Musical_Instruments.jsonl"), "Musical_Instruments")
    ]

    seen_signatures = set()
    category_stats = []
    overall_start = time.time()

    with open(out_file, "w", encoding="utf-8") as out_f:
        # Write TSV header
        out_f.write("asin\trating\tcategory\tverified\thelpful\tdate\tsentiment\ttext\n")
        for fpath, cat in files_to_process:
            if not os.path.exists(fpath):
                print(f"Warning: File not found: {fpath}. Skipping.")
                continue
            print(f"\n--> Cleaning file: {os.path.basename(fpath)} (Category: {cat})")
            stat = process_file(fpath, cat, out_f, seen_signatures)
            category_stats.append(stat)

    overall_elapsed = time.time() - overall_start
    total_in = sum(s["total_read"] for s in category_stats)
    total_out = sum(s["total_written"] for s in category_stats)
    out_size_bytes = os.path.getsize(out_file) if os.path.exists(out_file) else 0

    cleaning_report = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_input_records": total_in,
        "total_processed_records": total_out,
        "processed_file_size_bytes": out_size_bytes,
        "processed_file_size_gb": round(out_size_bytes / (1024**3), 3),
        "total_elapsed_seconds": round(overall_elapsed, 2),
        "per_category_statistics": category_stats
    }

    report_path = os.path.join(reports_dir, "data_cleaning_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(cleaning_report, f, indent=2)

    print("\n" + "=" * 70)
    print("DATA CLEANING AUDIT REPORT")
    print(f"Total Input Raw Records:       {total_in:,}")
    print(f"Total Cleaned Records Output:   {total_out:,}")
    print(f"Cleaned TSV File Size:         {cleaning_report['processed_file_size_gb']} GB ({out_size_bytes:,} bytes)")
    print(f"Total Cleaning Time:           {overall_elapsed:.2f} seconds")
    print(f"Saved Cleaning Report:         {report_path}")
    print("=" * 70)

if __name__ == "__main__":
    main()
