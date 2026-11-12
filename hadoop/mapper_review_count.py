#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 2: Most Reviewed Products - Mapper
Extracts product ID from TSV input.
Emits: asin \t 1
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line.startswith("asin\trating"):
        continue
    parts = line.split("\t")
    if len(parts) >= 1:
        asin = parts[0].strip()
        if asin:
            print(f"{asin}\t1")
