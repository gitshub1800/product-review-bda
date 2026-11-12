#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 2: Most Reviewed Products - Reducer
Aggregates total reviews for each product ID.
Emits: Product ID \t Total Reviews
"""
import sys

current_asin = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue
    asin = parts[0].strip()
    try:
        count = int(parts[1].strip())
    except ValueError:
        continue

    if current_asin == asin:
        current_count += count
    else:
        if current_asin:
            print(f"{current_asin}\t{current_count}")
        current_asin = asin
        current_count = count

if current_asin:
    print(f"{current_asin}\t{current_count}")
