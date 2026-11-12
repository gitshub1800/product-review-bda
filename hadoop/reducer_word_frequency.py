#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 4: Word Frequency Analysis - Reducer
Aggregates word occurrences across all customer reviews.
Emits: Word \t Frequency
"""
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue
    word = parts[0].strip()
    try:
        count = int(parts[1].strip())
    except ValueError:
        continue

    if current_word == word:
        current_count += count
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word:
    print(f"{current_word}\t{current_count}")
