"""
BDA Mini Practice Case Study - Master Automation Coordinator
Coordinates end-to-end execution:
1. Ingests and downloads Amazon Reviews 2023 dataset (>2 GB).
2. Performs validation and schema audit.
3. Cleans and normalizes dataset in streaming mode.
4. Executes 4 Hadoop MapReduce Streaming jobs on Apache Hadoop 3.3.6.
5. Trains and evaluates Multinomial Naive Bayes ML model.
6. Generates analytical result summaries and reports.
"""

import os
import sys
import subprocess
import time

PYTHON_EXE = r"C:\Users\shaik\.gemini\antigravity\scratch\tools\python311\python.exe"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_step(step_name, script_rel_path):
    print("\n" + "=" * 80)
    print(f"STEP: {step_name}")
    print(f"Executing: {script_rel_path}")
    print("=" * 80)

    script_path = os.path.join(BASE_DIR, script_rel_path)
    start = time.time()
    res = subprocess.run([PYTHON_EXE, script_path], cwd=BASE_DIR)
    duration = time.time() - start

    if res.returncode != 0:
        print(f"\n[ERROR] Step '{step_name}' failed with exit code {res.returncode}")
        sys.exit(res.returncode)

    print(f"[SUCCESS] Step '{step_name}' completed in {duration:.2f} seconds.")
    return duration

def main():
    print("#" * 80)
    print("GOOGLE ANTIGRAVITY - COMPLETE BDA MINI PRACTICE CASE STUDY")
    print("Topic: Product Review Analytics and Rating Aggregation Using Hadoop & MapReduce")
    print("Assignment Number: 11.7")
    print("#" * 80)

    timings = {}
    total_start = time.time()

    # Step 1: Download Dataset
    timings["download"] = run_step(
        "1. Real Dataset Download (>2 GB)",
        os.path.join("preprocessing", "download_dataset.py")
    )

    # Step 2: Validate Dataset
    timings["validate"] = run_step(
        "2. Dataset Validation & Schema Inspection",
        os.path.join("preprocessing", "validate_dataset.py")
    )

    # Step 3: Clean & Preprocess Data
    timings["clean"] = run_step(
        "3. Streaming Data Cleaning & Normalization",
        os.path.join("preprocessing", "clean_data.py")
    )

    # Step 4: Execute Hadoop MapReduce Jobs
    timings["mapreduce"] = run_step(
        "4. Hadoop MapReduce Jobs Execution (4 Algorithms)",
        os.path.join("hadoop", "run_mapreduce_jobs.py")
    )

    # Step 5: Machine Learning Training & Evaluation
    timings["ml"] = run_step(
        "5. Sentiment Classification Machine Learning (Naive Bayes)",
        os.path.join("machine_learning", "train_model.py")
    )

    # Step 6: Test Custom Review Predictions
    timings["eval"] = run_step(
        "6. Model Prediction Smoke Testing",
        os.path.join("machine_learning", "evaluate_model.py")
    )

    total_duration = time.time() - total_start

    print("\n" + "#" * 80)
    print("ALL PROJECT PIPELINE STEPS COMPLETED SUCCESSFULLY!")
    print(f"Total Pipeline Runtime: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    print("#" * 80)
    print("\nTo launch the interactive dashboard, run:")
    print(f'"{PYTHON_EXE}" -m streamlit run "{os.path.join(BASE_DIR, "dashboard", "app.py")}"\n')

if __name__ == "__main__":
    main()
