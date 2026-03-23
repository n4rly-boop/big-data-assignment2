import subprocess
import sys
import time
from cassandra.cluster import Cluster


def wait_for_cassandra():
    # cassandra takes a while to start up
    for i in range(20):
        try:
            c = Cluster(['cassandra-server'])
            c.connect()
            c.shutdown()
            return
        except Exception:
            print(f"waiting for cassandra... {i+1}")
            time.sleep(5)
    print("cassandra not available")
    sys.exit(1)


def read_hdfs(path):
    result = subprocess.run(
        f"hdfs dfs -cat {path}",
        shell=True, capture_output=True, text=True, check=True
    )
    return result.stdout.strip().split("\n")


def main():
    wait_for_cassandra()

    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()

    # create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('search_engine')

    # recreate tables
    session.execute("DROP TABLE IF EXISTS documents")
    session.execute("DROP TABLE IF EXISTS inverted_index")
    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    session.execute("""
        CREATE TABLE documents (
            doc_id INT PRIMARY KEY,
            title TEXT,
            dl INT
        )
    """)
    session.execute("""
        CREATE TABLE inverted_index (
            term TEXT,
            doc_id INT,
            tf INT,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE vocabulary (
            term TEXT PRIMARY KEY,
            df INT
        )
    """)
    session.execute("""
        CREATE TABLE corpus_stats (
            id INT PRIMARY KEY,
            num_docs INT,
            avg_dl FLOAT
        )
    """)
    print("tables created")

    # load doc stats
    print("loading doc stats...")
    lines = read_hdfs("/tmp/indexer/doc_stats/part-*")

    ins_doc = session.prepare("INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)")
    ins_stats = session.prepare("INSERT INTO corpus_stats (id, num_docs, avg_dl) VALUES (?, ?, ?)")

    for line in lines:
        if not line.strip():
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        if parts[0] == "__STATS__":
            n = int(parts[1])
            avg = float(parts[2])
            session.execute(ins_stats, (1, n, avg))
            print(f"  stats: N={n}, avg_dl={avg:.2f}")
        else:
            session.execute(ins_doc, (int(parts[0]), parts[1], int(parts[2])))

    # load inverted index
    print("loading inverted index...")
    lines = read_hdfs("/tmp/indexer/inverted_index/part-*")

    ins_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    ins_post = session.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")

    for line in lines:
        if not line.strip():
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        term = parts[0]
        df = int(parts[1])
        postings = parts[2]

        session.execute(ins_vocab, (term, df))
        for p in postings.split(","):
            did, tf = p.split(":")
            session.execute(ins_post, (term, int(did), int(tf)))

    # verify
    r = session.execute("SELECT COUNT(*) FROM documents")
    print(f"  documents: {r.one()[0]}")
    r = session.execute("SELECT COUNT(*) FROM vocabulary")
    print(f"  vocab terms: {r.one()[0]}")

    session.shutdown()
    cluster.shutdown()
    print("done loading index")


if __name__ == "__main__":
    main()
