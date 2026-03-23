#!/usr/bin/env python3
import sys
import re
from collections import Counter

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    doc_id = parts[0]
    title = parts[1]
    text = parts[2]

    # lowercase and remove punctuation
    text_clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
    words = text_clean.split()

    tf_counts = Counter(words)
    for term, tf in tf_counts.items():
        print(f"{term}\t{doc_id}\t{tf}")
