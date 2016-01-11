#!/usr/bin/python
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

master = sys.argv[1] 

sc = SparkContext(master, "IRC streaming")
ssc = StreamingContext(sc, 10)

# run in another shell:
# ./irc_wiki.sh | nc -lk 9999
nc = sys.argv[2] #"10.0.0.1"

nc_port = sys.argv[3] #9999

lines = ssc.socketTextStream(nc, nc_port)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.saveAsTextFiles("hdfs:///tmp/irc-data")

#pairs.pprint()

ssc.start()

ssc.awaitTermination()

