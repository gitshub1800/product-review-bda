"""
BDA Project - Hadoop MapReduce Job Coordinator
Executes all 4 Hadoop Streaming MapReduce jobs using Apache Hadoop 3.3.6.
Tracks execution times, captures counters, and formats results.
"""

import os
import sys
import subprocess
import time
import json
import shutil

HADOOP_HOME = r"C:\Users\shaik\.gemini\antigravity\scratch\tools\hadoop-3.3.6"
JAVA_HOME = r"C:\Users\shaik\.gemini\antigravity\scratch\tools\jdk-11"
PYTHON_EXE = r"C:\Users\shaik\.gemini\antigravity\scratch\tools\python311\python.exe"

STREAMING_JAR = os.path.join(HADOOP_HOME, "share", "hadoop", "tools", "lib", "hadoop-streaming-3.3.6.jar")
HADOOP_BIN = os.path.join(HADOOP_HOME, "bin", "hadoop.cmd")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROCESSED_DATA = os.path.join(BASE_DIR, "dataset", "processed", "cleaned_reviews.tsv")
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

JOBS = [
    {
        "id": "job1_average_rating",
        "name": "Algorithm 1: Average Product Rating",
        "mapper": "mapper_average.py",
        "reducer": "reducer_average.py",
        "output_subdir": "output_avg_rating",
        "result_file": "algorithm_1_average_ratings.tsv"
    },
    {
        "id": "job2_most_reviewed",
        "name": "Algorithm 2: Most Reviewed Products",
        "mapper": "mapper_review_count.py",
        "reducer": "reducer_review_count.py",
        "output_subdir": "output_review_count",
        "result_file": "algorithm_2_most_reviewed_all.tsv"
    },
    {
        "id": "job3_category_analysis",
        "name": "Algorithm 3: Category-Wise Rating Analysis",
        "mapper": "mapper_category.py",
        "reducer": "reducer_category.py",
        "output_subdir": "output_category",
        "result_file": "algorithm_3_category_analysis.tsv"
    },
    {
        "id": "job4_word_frequency",
        "name": "Algorithm 4: Word Frequency Analysis",
        "mapper": "mapper_word_frequency.py",
        "reducer": "reducer_word_frequency.py",
        "output_subdir": "output_word_frequency",
        "result_file": "algorithm_4_word_frequency_all.tsv",
        "custom_input": os.path.join(BASE_DIR, "dataset", "processed", "cleaned_reviews_sample.tsv")
    }
]

def run_job(job, input_path):
    print("\n" + "=" * 70)
    print(f"RUNNING HADOOP MAPREDUCE: {job['name']}")
    print("=" * 70)

    # Hadoop output directory
    hadoop_out = os.path.join(BASE_DIR, "dataset", "hadoop_output", job["output_subdir"]).replace("\\", "/")
    if os.path.exists(hadoop_out):
        shutil.rmtree(hadoop_out)

    mapper_path = os.path.join(HADOOP_DIR, job["mapper"])
    reducer_path = os.path.join(HADOOP_DIR, job["reducer"])

    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    env["HADOOP_HOME"] = HADOOP_HOME
    env["PATH"] = f"{os.path.dirname(PYTHON_EXE)};{HADOOP_HOME}\\bin;{JAVA_HOME}\\bin;" + env.get("PATH", "")

    # Input URI
    in_uri = "file:///" + input_path.replace("\\", "/")
    out_uri = "file:///" + hadoop_out

    cmd = [
        HADOOP_BIN,
        "jar",
        STREAMING_JAR,
        "-input", in_uri,
        "-output", out_uri,
        "-mapper", f"python {job['mapper']}",
        "-reducer", f"python {job['reducer']}",
        "-file", mapper_path,
        "-file", reducer_path
    ]

    print("Command:", " ".join(cmd))
    start_time = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, errors="replace")

    output_lines = []
    for line in proc.stdout:
        output_lines.append(line)
        if "INFO" in line and ("Job" in line or "map" in line or "reduce" in line or "Counters" in line):
            print("  [HADOOP]", line.strip())

    proc.wait()
    duration = time.time() - start_time

    if proc.returncode != 0:
        print(f"  [ERROR] Job {job['id']} failed with code {proc.returncode}")
        return {
            "job_id": job["id"],
            "name": job["name"],
            "status": "FAILED",
            "duration_seconds": round(duration, 2),
            "output_lines": output_lines[-20:]
        }

    # Locate part-00000
    part_file = os.path.join(hadoop_out, "part-00000")
    if not os.path.exists(part_file):
        # Check subdirectories
        for root, _, files in os.walk(hadoop_out):
            if "part-00000" in files:
                part_file = os.path.join(root, "part-00000")
                break

    dest_result = os.path.join(RESULTS_DIR, job["result_file"])
    if os.path.exists(part_file):
        shutil.copyfile(part_file, dest_result)
        out_records = sum(1 for _ in open(dest_result, "r", encoding="utf-8", errors="ignore"))
        print(f"  [SUCCESS] MapReduce Job Finished in {duration:.2f}s | Output Records: {out_records:,}")
    else:
        out_records = 0
        print(f"  [WARN] Output file part-00000 not found in {hadoop_out}")

    return {
        "job_id": job["id"],
        "name": job["name"],
        "status": "SUCCESS",
        "duration_seconds": round(duration, 2),
        "output_records": out_records,
        "result_file": dest_result
    }

def post_process_top_results():
    """Extracts top 10 most reviewed products and top 50 frequent words."""
    # Top 10 Most Reviewed
    job2_all = os.path.join(RESULTS_DIR, "algorithm_2_most_reviewed_all.tsv")
    top10_file = os.path.join(RESULTS_DIR, "algorithm_2_top10_most_reviewed.tsv")
    if os.path.exists(job2_all):
        items = []
        with open(job2_all, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) == 2:
                    try:
                        items.append((parts[0], int(parts[1])))
                    except ValueError:
                        pass
        items.sort(key=lambda x: x[1], reverse=True)
        with open(top10_file, "w", encoding="utf-8") as f:
            f.write("asin\ttotal_reviews\n")
            for asin, cnt in items[:10]:
                f.write(f"{asin}\t{cnt}\n")
        print(f"Generated Top 10 Most Reviewed Products: {top10_file}")

    # Top 50 Frequent Words
    job4_all = os.path.join(RESULTS_DIR, "algorithm_4_word_frequency_all.tsv")
    top50_words = os.path.join(RESULTS_DIR, "algorithm_4_top50_words.tsv")
    if os.path.exists(job4_all):
        words = []
        with open(job4_all, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) == 2:
                    try:
                        words.append((parts[0], int(parts[1])))
                    except ValueError:
                        pass
        words.sort(key=lambda x: x[1], reverse=True)
        with open(top50_words, "w", encoding="utf-8") as f:
            f.write("word\tfrequency\n")
            for w, cnt in words[:50]:
                f.write(f"{w}\t{cnt}\n")
        print(f"Generated Top 50 Word Frequencies: {top50_words}")

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    if not os.path.exists(PROCESSED_DATA):
        print(f"Processed dataset not found at {PROCESSED_DATA}. Please run clean_data.py first.")
        sys.exit(1)

    print("=" * 70)
    print("APACHE HADOOP 3.3.6 MAPREDUCE BATCH RUNNER")
    print(f"Input Dataset: {PROCESSED_DATA} ({os.path.getsize(PROCESSED_DATA) / (1024**2):.2f} MB)")
    print("=" * 70)

    job_reports = []
    overall_start = time.time()

    for job in JOBS:
        target_in = job.get("custom_input", PROCESSED_DATA)
        rep = run_job(job, target_in)
        job_reports.append(rep)

    post_process_top_results()
    overall_time = time.time() - overall_start

    summary = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "hadoop_version": "Apache Hadoop 3.3.6",
        "mode": "Pseudo-distributed / Native MapReduce Streaming",
        "total_jobs_executed": len(JOBS),
        "successful_jobs": sum(1 for j in job_reports if j["status"] == "SUCCESS"),
        "total_mapreduce_duration_seconds": round(overall_time, 2),
        "job_details": job_reports
    }

    report_path = os.path.join(REPORTS_DIR, "mapreduce_execution_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n" + "=" * 70)
    print("MAPREDUCE EXECUTION COMPLETED")
    print(f"Total Duration: {overall_time:.2f} seconds")
    print(f"Report saved to: {report_path}")
    print("=" * 70)

if __name__ == "__main__":
    main()
