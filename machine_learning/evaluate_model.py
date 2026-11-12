"""
BDA Project - Sentiment Model Evaluator & Predictor
Loads the trained Naive Bayes sentiment model and predicts sentiment for custom reviews.
"""

import os
import sys
import json
import joblib

def load_model_and_vectorizer(results_dir):
    model_path = os.path.join(results_dir, "sentiment_nb_model.joblib")
    vec_path = os.path.join(results_dir, "tfidf_vectorizer.joblib")
    if not os.path.exists(model_path) or not os.path.exists(vec_path):
        raise FileNotFoundError("Model or vectorizer not found. Run train_model.py first.")
    model = joblib.load(model_path)
    vec = joblib.load(vec_path)
    return model, vec

def predict_review_sentiment(review_text, model, vectorizer):
    feats = vectorizer.transform([review_text])
    pred = model.predict(feats)[0]
    probs = model.predict_proba(feats)[0]
    classes = list(model.classes_)
    confidence = max(probs)
    prob_dict = {cls: round(float(prob), 4) for cls, prob in zip(classes, probs)}
    return {
        "text": review_text,
        "sentiment": pred,
        "confidence": round(float(confidence), 4),
        "probabilities": prob_dict
    }

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    results_dir = os.path.join(base_dir, "results")

    model, vec = load_model_and_vectorizer(results_dir)

    test_reviews = [
        "This product exceeded all my expectations! Outstanding build quality and crystal clear sound.",
        "Terrible experience. The item arrived broken, completely unusable and seller refused return.",
        "It is an average product. Works okay for the price, nothing special.",
        "Absolute waste of money. Do not buy under any circumstances.",
        "Highly recommended to everyone! Amazing performance and very easy to set up."
    ]

    print("=" * 70)
    print("LIVE NAIVE BAYES SENTIMENT PREDICTION TEST")
    print("=" * 70)

    for rev in test_reviews:
        res = predict_review_sentiment(rev, model, vec)
        print(f"\nReview:     \"{res['text']}\"")
        print(f"Prediction: [{res['sentiment'].upper()}] (Confidence: {res['confidence']*100:.1f}%)")
        print(f"Probabilities: {res['probabilities']}")

if __name__ == "__main__":
    main()
