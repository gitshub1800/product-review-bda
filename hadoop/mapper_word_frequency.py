#!/usr/bin/env python3
"""
Hadoop MapReduce Algorithm 4: Word Frequency Analysis - Mapper
Extracts review text, filters stopwords and punctuation, and emits: word \t 1
"""
import sys
import re

STOPWORDS = {
    "the", "and", "is", "it", "this", "to", "for", "in", "of", "on", "with",
    "that", "as", "was", "are", "you", "my", "not", "have", "but", "very",
    "at", "be", "so", "or", "from", "an", "they", "just", "we", "if", "all",
    "one", "by", "out", "had", "about", "what", "there", "when", "which",
    "can", "would", "more", "like", "up", "will", "been", "their", "has",
    "he", "she", "no", "do", "get", "its", "also", "into", "than", "them"
}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if line.startswith("asin\trating"):
        continue
    parts = line.split("\t")
    if len(parts) >= 8:
        text = parts[7].strip()
        words = re.findall(r"[a-z]{3,}", text.lower())
        for word in words:
            if word not in STOPWORDS:
                print(f"{word}\t1")
