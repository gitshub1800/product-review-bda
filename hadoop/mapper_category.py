#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 3: Category-Wise Rating Analysis - Mapper
Extracts category and rating from TSV input.
Emits: category \t rating,1
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line.startswith("asin\trating"):
        continue
    parts = line.split("\t")
    if len(parts) >= 3:
        try:
            rating = float(parts[1].strip())
            category = parts[2].strip()
            if category and 1.0 <= rating <= 5.0:
                print(f"{category}\t{rating},1")
        except ValueError:
            continue
