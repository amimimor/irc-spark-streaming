#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

master = "yarn-client"

sc = SparkContext(master, "IRC streaming")
ssc = StreamingContext(sc, 10)

# run in another shell:
# ./irc_wiki.sh | nc -lk 9999

# nc is the address of the nc server
nc = "10.0.0.1"

lines = ssc.socketTextStream(nc, 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.saveAsTextFiles("hdfs:///tmp/irc-data")

#pairs.pprint()

ssc.start()

ssc.awaitTermination()

