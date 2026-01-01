import sys
import time
from pyspark.sql import SparkSession
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <input_path> <n_threads>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    n_threads = int(sys.argv[2])

    spark = SparkSession.builder.appName("WordCount").master("local[%d]" % n_threads).getOrCreate()

    start_time = time.time()

    lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

    counts = lines.flatMap(lambda s: s.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(add)

    output = counts.collect()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Job execution time: {execution_time:.4f} seconds")

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()