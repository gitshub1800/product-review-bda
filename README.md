# Product Review Analytics and Rating Aggregation Using Hadoop and MapReduce

**Assignment Number:** 11.7  
**Course:** Big Data Analytics (BDA) Mini Practice Case Study  
**Dataset:** Amazon Reviews 2023 (McAuley Lab, UCSD) — 2.316 GB Downloaded, 5,146,044 Real Customer Reviews  
**Framework:** Apache Hadoop 3.3.6 (HDFS & MapReduce Streaming) & Scikit-learn (Multinomial Naive Bayes)

---

## 1. System Architecture

```
                       Amazon Reviews 2023 Dataset (2.32 GB, 5.14M records)
                                                |
                                                v
                                  Dataset Download & Validation
                                                |
                                                v
                                  Data Cleaning & Preprocessing
                                    (1.35 GB Cleaned TSV)
                                                |
                                                v
                                       Hadoop HDFS Storage
                          (/bda_project/input/cleaned_reviews.tsv)
                                                |
                                                v
                              Hadoop Streaming MapReduce Execution
                     +--------------------------+--------------------------+
                     |                          |                          |
                     v                          v                          v
              [Algorithm 1]              [Algorithm 2]              [Algorithm 3]
          Average Product Rating     Most Reviewed Products    Category-Wise Ratings
                     |                          |                          |
                     +--------------------------+--------------------------+
                                                |
                                                v
                                          [Algorithm 4]
                                     Word Frequency Analysis
                                                |
                                                v
                                   Machine Learning Pipeline
                             (TF-IDF + Multinomial Naive Bayes)
                                (Accuracy: 90.19%, F1: 88.76%)
                                                |
                                                v
                               Streamlit Interactive Dashboard
                                     (http://localhost:8501)
```

---

## 2. Project Directory Structure

```
BDA_Product_Review_Analytics/
├── dataset/
│   ├── raw/
│   │   ├── Appliances.jsonl                  (886.39 MB, 2,128,605 reviews)
│   │   └── Musical_Instruments.jsonl         (1,485.68 MB, 3,017,439 reviews)
│   ├── processed/
│   │   ├── cleaned_reviews.tsv               (1.35 GB, 5,124,820 cleaned reviews)
│   │   └── cleaned_reviews_sample.tsv        (61.8 MB, 250,000 records)
│   └── hadoop_output/                        (Hadoop MapReduce intermediate outputs)
│
├── preprocessing/
│   ├── download_dataset.py                   (Resumable streaming chunk downloader)
│   ├── validate_dataset.py                   (Schema, integrity, and record counting)
│   └── clean_data.py                         (Regex cleaning, deduplication, TSV generation)
│
├── hadoop/
│   ├── mapper_average.py                     (Algorithm 1: Mapper for average rating)
│   ├── reducer_average.py                    (Algorithm 1: Reducer for average rating)
│   ├── mapper_review_count.py                (Algorithm 2: Mapper for product review count)
│   ├── reducer_review_count.py               (Algorithm 2: Reducer for product review count)
│   ├── mapper_category.py                    (Algorithm 3: Mapper for category rating)
│   ├── reducer_category.py                   (Algorithm 3: Reducer for category rating)
│   ├── mapper_word_frequency.py              (Algorithm 4: Mapper for word tokenization)
│   ├── reducer_word_frequency.py             (Algorithm 4: Reducer for word frequency)
│   └── run_mapreduce_jobs.py                 (Apache Hadoop Streaming batch runner)
│
├── machine_learning/
│   ├── train_model.py                        (Multinomial Naive Bayes trainer + TF-IDF)
│   └── evaluate_model.py                     (Live sentiment inference on custom reviews)
│
├── dashboard/
│   └── app.py                                (7-Page interactive Streamlit web dashboard)
│
├── results/
│   ├── algorithm_1_average_ratings.tsv       (ASIN | Total Reviews | Average Rating)
│   ├── algorithm_2_most_reviewed_all.tsv     (All product review counts)
│   ├── algorithm_2_top10_most_reviewed.tsv   (Top 10 most-reviewed products)
│   ├── algorithm_3_category_analysis.tsv     (Category | Total Reviews | Average Rating)
│   ├── algorithm_4_top50_words.tsv           (Top 50 frequent review words)
│   ├── algorithm_4_word_frequency_all.tsv    (47,264 unique word frequencies)
│   ├── sentiment_nb_model.joblib             (Serialized trained ML model)
│   ├── tfidf_vectorizer.joblib               (Serialized TF-IDF vectorizer)
│   └── ml_evaluation_metrics.json            (Accuracy, Precision, Recall, F1, Confusion Matrix)
│
├── reports/
│   ├── dataset_validation_report.json        (Schema & file audit report)
│   ├── data_cleaning_report.json             (Deduplication & normalization audit)
│   ├── mapreduce_execution_report.json       (Hadoop job duration & record counters)
│   └── performance_analysis_report.json      (End-to-end benchmarking data)
│
├── requirements.txt                          (Python dependencies)
├── run_project.py                            (End-to-end master pipeline coordinator)
└── README.md                                 (Project documentation & execution guide)
```

---

## 3. How to Run the Project

### Prerequisites
- Python 3.11+
- Java OpenJDK 11
- Apache Hadoop 3.3.6

### 1. Run Complete Automation Pipeline
To run all steps (download, validation, cleaning, Hadoop MapReduce, and ML training):
```powershell
python run_project.py
```

### 2. Run Individual Modules
- **Download Dataset (>2 GB):**
  ```powershell
  python preprocessing/download_dataset.py
  ```
- **Validate Dataset:**
  ```powershell
  python preprocessing/validate_dataset.py
  ```
- **Clean & Preprocess Data:**
  ```powershell
  python preprocessing/clean_data.py
  ```
- **Execute 4 Hadoop MapReduce Jobs:**
  ```powershell
  python hadoop/run_mapreduce_jobs.py
  ```
- **Train Sentiment ML Model:**
  ```powershell
  python machine_learning/train_model.py
  ```
- **Test Sentiment Predictions:**
  ```powershell
  python machine_learning/evaluate_model.py
  ```

### 3. Launch the Streamlit Web Dashboard
```powershell
streamlit run dashboard/app.py
```
Open your browser at `http://localhost:8501`.

---

## 4. Key Performance Results

| Metric | Measured Value |
| :--- | :--- |
| **Raw Dataset Downloaded** | **2.316 GB** (2,487,296,519 bytes) |
| **Total Real Customer Reviews** | **5,146,044** |
| **Cleaned Reviews** | **5,124,820** (1.35 GB TSV) |
| **HDFS Upload Time** | 18.2 seconds |
| **Hadoop MapReduce Execution Time** | 244.21 seconds (4 algorithms) |
| **ML Model Accuracy** | **90.19%** |
| **ML Weighted F1-Score** | **88.76%** |
