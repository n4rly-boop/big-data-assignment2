from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('data transformation') \
    .master("local") \
    .getOrCreate()

# read all txt files from hdfs
rdd = spark.sparkContext.wholeTextFiles("hdfs:///data/*.txt")

def parse_file(pair):
    filepath, content = pair
    fname = filepath.split("/")[-1].replace(".txt", "")
    parts = fname.split("_", 1)
    doc_id = parts[0]
    title = parts[1].replace("_", " ") if len(parts) > 1 else ""
    # clean up tabs and newlines so tsv doesnt break
    clean = content.replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()
    return f"{doc_id}\t{title}\t{clean}"

result = rdd.map(parse_file).coalesce(1)
result.saveAsTextFile("hdfs:///input/data")

spark.stop()
print("done")
