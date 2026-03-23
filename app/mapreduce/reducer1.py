#!/usr/bin/env python3
import sys

total_dl = 0
num_docs = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    doc_id = parts[0]
    title = parts[1]
    dl = int(parts[2])

    # pass through the line
    print(f"{doc_id}\t{title}\t{dl}")

    total_dl += dl
    num_docs += 1

# print avg stats
avg_dl = total_dl / num_docs if num_docs > 0 else 0
print(f"__STATS__\t{num_docs}\t{avg_dl:.4f}")
