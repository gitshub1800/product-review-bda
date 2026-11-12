#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 1: Average Product Rating - Reducer
Aggregates rating sum and review count for each product.
Emits: Product ID \t Total Reviews \t Average Rating
"""
import sys

current_asin = None
total_rating = 0.0
total_reviews = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue
    asin = parts[0].strip()
    val_parts = parts[1].strip().split(",")
    if len(val_parts) != 2:
        continue
    try:
        rating = float(val_parts[0])
        count = int(val_parts[1])
    except ValueError:
        continue

    if current_asin == asin:
        total_rating += rating
        total_reviews += count
    else:
        if current_asin:
            avg_rating = total_rating / total_reviews if total_reviews > 0 else 0.0
            print(f"{current_asin}\t{total_reviews}\t{avg_rating:.2f}")
        current_asin = asin
        total_rating = rating
        total_reviews = count

if current_asin:
    avg_rating = total_rating / total_reviews if total_reviews > 0 else 0.0
    print(f"{current_asin}\t{total_reviews}\t{avg_rating:.2f}")
