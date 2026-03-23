import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster


def preprocess(text):
    clean = re.sub(r'[^a-z0-9\s]', '', text.lower())
    return [w for w in clean.split() if w]


def get_data_from_cassandra(terms):
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    # corpus stats
    row = session.execute("SELECT num_docs, avg_dl FROM corpus_stats WHERE id = 1").one()
    N = row.num_docs
    avg_dl = row.avg_dl

    # get df for each term
    vocab = {}
    for t in terms:
        r = session.execute("SELECT df FROM vocabulary WHERE term = %s", (t,)).one()
        if r:
            vocab[t] = r.df

    # get postings
    postings = []
    for t in terms:
        if t not in vocab:
            continue
        rows = session.execute("SELECT doc_id, tf FROM inverted_index WHERE term = %s", (t,))
        for r in rows:
            postings.append((t, r.doc_id, r.tf))

    # get doc info for candidates
    doc_ids = set(did for _, did, _ in postings)
    docs = {}
    for did in doc_ids:
        r = session.execute("SELECT title, dl FROM documents WHERE doc_id = %s", (did,)).one()
        if r:
            docs[did] = (r.title, r.dl)

    session.shutdown()
    cluster.shutdown()
    return N, avg_dl, vocab, postings, docs


def bm25_search(terms, N, avg_dl, vocab, postings, docs, k1=1.5, b=0.75):
    conf = SparkConf().setAppName("BM25Search")
    sc = SparkContext(conf=conf)

    try:
        # broadcast lookup data to workers
        bv = sc.broadcast(vocab)
        bd = sc.broadcast(docs)
        bN = sc.broadcast(N)
        ba = sc.broadcast(avg_dl)

        rdd = sc.parallelize(postings)

        def score(posting):
            term, doc_id, tf = posting
            df = bv.value.get(term, 1)
            doc = bd.value.get(doc_id)
            if not doc:
                return (doc_id, 0.0)
            title, dl = doc
            # BM25 formula
            idf = math.log(bN.value / max(df, 1))
            num = (k1 + 1) * tf
            denom = k1 * ((1 - b) + b * (dl / ba.value)) + tf
            return (doc_id, idf * num / denom)

        results = (
            rdd
            .map(score)
            .reduceByKey(lambda x, y: x + y)
            .sortBy(lambda x: x[1], ascending=False)
            .take(10)
        )
        return results
    finally:
        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: spark-submit query.py \"query\"", file=sys.stderr)
        sys.exit(1)

    query = " ".join(sys.argv[1:])

    terms = preprocess(query)
    if not terms:
        print("no valid terms")
        sys.exit(0)

    N, avg_dl, vocab, postings, docs = get_data_from_cassandra(terms)

    if not postings:
        print("no matching documents")
        sys.exit(0)

    results = bm25_search(terms, N, avg_dl, vocab, postings, docs)

    for doc_id, sc in results:
        title = docs[doc_id][0]
        print(f"{doc_id}\t{title}")
