#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 1: Average Product Rating - Mapper
Reads TSV records from STDIN.
Emits: asin \t rating,1
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    # Skip header
    if line.startswith("asin\trating"):
        continue
    parts = line.split("\t")
    if len(parts) >= 2:
        asin = parts[0].strip()
        try:
            rating = float(parts[1].strip())
            if asin and 1.0 <= rating <= 5.0:
                print(f"{asin}\t{rating},1")
        except ValueError:
            continue
