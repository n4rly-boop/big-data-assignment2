#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    doc_id = parts[0]
    title = parts[1]
    text = parts[2]

    # preprocess
    text_clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
    words = text_clean.split()
    dl = len(words)

    print(f"{doc_id}\t{title}\t{dl}")
