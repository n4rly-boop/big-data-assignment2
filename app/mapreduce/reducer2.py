#!/usr/bin/env python3
import sys

current_term = None
postings = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    term = parts[0]
    doc_id = parts[1]
    tf = parts[2]

    if term != current_term:
        # emit previous term
        if current_term is not None:
            df = len(postings)
            postings_str = ','.join(postings)
            print(f"{current_term}\t{df}\t{postings_str}")
        current_term = term
        postings = []

    postings.append(f"{doc_id}:{tf}")

# last one
if current_term is not None:
    df = len(postings)
    postings_str = ','.join(postings)
    print(f"{current_term}\t{df}\t{postings_str}")
