"""
BDA Project - Machine Learning Sentiment Classifier
Trains a Multinomial Naive Bayes model on review text using TF-IDF features.
Labels:
  - 1.0 - 2.0 stars -> negative
  - 3.0 stars       -> neutral
  - 4.0 - 5.0 stars -> positive
"""

import os
import sys
import time
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)

def train_sentiment_model(input_file, results_dir, sample_size=100000, random_state=42):
    os.makedirs(results_dir, exist_ok=True)

    print("=" * 70)
    print("MACHINE LEARNING: NAIVE BAYES SENTIMENT CLASSIFICATION")
    print(f"Reading dataset: {input_file}")
    print("=" * 70)

    start_time = time.time()

    # Read data in streaming chunks to build a balanced/representative sample for ML
    texts = []
    labels = []
    ratings = []
    categories = []

    print(f"Sampling up to {sample_size:,} reviews for model training and evaluation...")
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        header = f.readline()
        for i, line in enumerate(f):
            parts = line.strip().split("\t")
            if len(parts) >= 8:
                rating_str, cat, sent, text = parts[1], parts[2], parts[6], parts[7]
                if text and sent in ("negative", "neutral", "positive"):
                    texts.append(text)
                    labels.append(sent)
                    ratings.append(float(rating_str))
                    categories.append(cat)
                    if len(texts) >= sample_size:
                        break

    print(f"Loaded {len(texts):,} reviews. Label distribution:")
    label_series = pd.Series(labels)
    for lbl, count in label_series.value_counts().items():
        print(f"  - {lbl.capitalize()}: {count:,} ({count / len(labels) * 100:.1f}%)")

    # 1. Stratified Train / Test Split
    print("\n[1/4] Splitting train (80%) and test (20%) datasets (stratified)...")
    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        texts, labels, test_size=0.20, random_state=random_state, stratify=labels
    )
    print(f"  Training samples: {len(X_train_raw):,}")
    print(f"  Testing samples:  {len(X_test_raw):,}")

    # 2. TF-IDF Feature Extraction
    print("\n[2/4] Extracting TF-IDF features (max_features=15,000, ngram_range=(1,2))...")
    vectorizer = TfidfVectorizer(
        max_features=15000,
        ngram_range=(1, 2),
        sublinear_tf=True,
        min_df=2
    )
    t0 = time.time()
    X_train = vectorizer.fit_transform(X_train_raw)
    X_test = vectorizer.transform(X_test_raw)
    feat_time = time.time() - t0
    print(f"  Feature matrix shape: {X_train.shape} in {feat_time:.2f}s")

    # 3. Multinomial Naive Bayes Training
    print("\n[3/4] Training Multinomial Naive Bayes classifier...")
    t0 = time.time()
    model = MultinomialNB(alpha=0.5)
    model.fit(X_train, y_train)
    train_time = time.time() - t0
    print(f"  Model trained in {train_time:.2f}s")

    # 4. Evaluation
    print("\n[4/4] Evaluating model performance...")
    t0 = time.time()
    y_pred = model.predict(X_test)
    eval_time = time.time() - t0

    acc = accuracy_score(y_test, y_pred)
    prec_macro = precision_score(y_test, y_pred, average="macro", zero_division=0)
    rec_macro = recall_score(y_test, y_pred, average="macro", zero_division=0)
    f1_macro = f1_score(y_test, y_pred, average="macro", zero_division=0)

    prec_weighted = precision_score(y_test, y_pred, average="weighted", zero_division=0)
    rec_weighted = recall_score(y_test, y_pred, average="weighted", zero_division=0)
    f1_weighted = f1_score(y_test, y_pred, average="weighted", zero_division=0)

    cm_labels = ["positive", "neutral", "negative"]
    cm = confusion_matrix(y_test, y_pred, labels=cm_labels).tolist()
    clf_rep = classification_report(y_test, y_pred, target_names=cm_labels, output_dict=True)

    print("\n" + "=" * 50)
    print("MODEL EVALUATION RESULTS")
    print(f"Accuracy:           {acc * 100:.2f}%")
    print(f"Precision (Macro):  {prec_macro * 100:.2f}%")
    print(f"Recall (Macro):     {rec_macro * 100:.2f}%")
    print(f"F1-Score (Macro):   {f1_macro * 100:.2f}%")
    print(f"Precision (Weight): {prec_weighted * 100:.2f}%")
    print(f"Recall (Weight):    {rec_weighted * 100:.2f}%")
    print(f"F1-Score (Weight):  {f1_weighted * 100:.2f}%")
    print("=" * 50)
    print("\nConfusion Matrix (Rows=Actual, Cols=Predicted):")
    print(f"{'':10} Positive   Neutral    Negative")
    for row_name, row_vals in zip(cm_labels, cm):
        print(f"{row_name.capitalize():10} {row_vals[0]:8d}   {row_vals[1]:8d}   {row_vals[2]:8d}")

    total_time = time.time() - start_time

    # Save artifacts
    model_path = os.path.join(results_dir, "sentiment_nb_model.joblib")
    vec_path = os.path.join(results_dir, "tfidf_vectorizer.joblib")
    metrics_path = os.path.join(results_dir, "ml_evaluation_metrics.json")

    joblib.dump(model, model_path)
    joblib.dump(vectorizer, vec_path)

    metrics_data = {
        "algorithm": "Multinomial Naive Bayes",
        "sample_size": len(texts),
        "train_size": len(X_train_raw),
        "test_size": len(X_test_raw),
        "accuracy": round(acc, 4),
        "precision_macro": round(prec_macro, 4),
        "recall_macro": round(rec_macro, 4),
        "f1_macro": round(f1_macro, 4),
        "precision_weighted": round(prec_weighted, 4),
        "recall_weighted": round(rec_weighted, 4),
        "f1_weighted": round(f1_weighted, 4),
        "training_time_seconds": round(train_time, 3),
        "evaluation_time_seconds": round(eval_time, 3),
        "total_time_seconds": round(total_time, 2),
        "confusion_matrix_labels": cm_labels,
        "confusion_matrix": cm,
        "classification_report": clf_rep
    }

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, indent=2)

    print(f"\nSaved model:      {model_path}")
    print(f"Saved vectorizer: {vec_path}")
    print(f"Saved metrics:    {metrics_path}")
    return metrics_data

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    input_file = os.path.join(base_dir, "dataset", "processed", "cleaned_reviews.tsv")
    results_dir = os.path.join(base_dir, "results")

    if not os.path.exists(input_file):
        print(f"Input file not found at {input_file}. Run data cleaning first.")
        sys.exit(1)

    train_sentiment_model(input_file, results_dir)

if __name__ == "__main__":
    main()
