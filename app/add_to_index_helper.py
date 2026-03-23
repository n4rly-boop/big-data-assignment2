import sys
import re
import os
from collections import Counter
from cassandra.cluster import Cluster


def preprocess(text):
    clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
    return [w for w in clean.split() if w]


def main():
    if len(sys.argv) < 2:
        print("usage: python3 add_to_index_helper.py <file>", file=sys.stderr)
        sys.exit(1)

    fpath = sys.argv[1]

    # get doc_id and title from filename
    base = os.path.basename(fpath).replace(".txt", "")
    parts = base.split("_", 1)
    doc_id = int(parts[0])
    title = parts[1].replace("_", " ") if len(parts) > 1 else ""

    with open(fpath) as f:
        text = f.read()

    words = preprocess(text)
    dl = len(words)
    tf_counts = Counter(words)

    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    # get current stats
    row = session.execute("SELECT num_docs, avg_dl FROM corpus_stats WHERE id = 1").one()
    old_n = row.num_docs
    old_avg = row.avg_dl

    # add document
    session.execute(
        "INSERT INTO documents (doc_id, title, dl) VALUES (%s, %s, %s)",
        (doc_id, title, dl)
    )

    # update index
    for term, tf in tf_counts.items():
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf)
        )
        # update df
        r = session.execute("SELECT df FROM vocabulary WHERE term = %s", (term,)).one()
        new_df = (r.df + 1) if r else 1
        session.execute(
            "INSERT INTO vocabulary (term, df) VALUES (%s, %s)",
            (term, new_df)
        )

    # update corpus stats
    new_n = old_n + 1
    new_avg = ((old_avg * old_n) + dl) / new_n
    session.execute(
        "INSERT INTO corpus_stats (id, num_docs, avg_dl) VALUES (%s, %s, %s)",
        (1, new_n, new_avg)
    )

    print(f"added [{doc_id}] {title} ({dl} words, {len(tf_counts)} terms)")
    print(f"corpus: N={new_n}, avg_dl={new_avg:.2f}")

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
