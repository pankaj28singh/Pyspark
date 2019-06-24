from __future__ import print_function
import os
import sys
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext



if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: ip.py <zk> <topic>", file=sys.stderr)
		exit(-1)
	sc = SparkContext(appName="PythonStreamingKafkaWordCount")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc,5)
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	lines = kvs.map(lambda x: x[1])
	words = lines.flatMap(lambda line: line.split(" "))
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)
	wordCounts.pprint()
	wordCounts.saveAsTextFiles("C:/Users/pansingh5/Desktop/spark-python/streaming-output/")
	ssc.start()
	ssc.awaitTermination()
	