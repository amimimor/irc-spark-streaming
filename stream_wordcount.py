#!/usr/bin/python
import sys
# if you are running in standalone and want to run as a __main__ or unittest
# you need to add SPARK_HOME to PYTHONPATH so that pyspark module 
# would be available
# you'd also need to do 'pip install py4j'
from pyspark import SparkContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

master = sys.argv[1] 
# create the SparkContext with 'master' which is build using a user param
sc = SparkContext(master, "streaming wordcount")
# crate a Streaming Spark Context with 'batch interval' of 10 seconds
ssc = StreamingContext(sc, 10)

# run in another shell:
# nc -lk 9999

# specify using user param the address of the nc server
nc = sys.argv[2] #"10.0.0.1"
# specify using user param the port of the nc server
nc_port = sys.argv[3] #9999
# read from socket as a stream of text.
# lines is type DStream[string]
lines = ssc.socketTextStream(nc, nc_port)
# words is DStream[string] of tokenized lines that result in 'bag of words'
words = lines.flatMap(lambda line: line.split(" "))
# pairs is DStream[(string,int)]
pairs = words.map(lambda word: (word, 1))
# wordCounts is DStream[(string,int)] where the int is the sum per string word
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
# print the results to target dir prefixed by the given string arg
wordCounts.pprint()
# start the streaming application
ssc.start()
# terminate it when the user sends a termination signal
ssc.awaitTermination()

