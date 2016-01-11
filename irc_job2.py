#!/usr/bin/python
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def get_src(lines):
       if len(lines) > 2:
          src = lines[2].lstrip("#")
          return src
       return [""]

def parse_line(s):
       lines = s.split(" ")
       src = get_src(lines)
       return lines

master = sys.argv[1] 

sc = SparkContext(master, "IRC streaming")
ssc = StreamingContext(sc, 10)

# run in another shell:
# ./irc_wiki.sh | nc -lk 9999
nc = sys.argv[2] #"10.0.0.1"

nc_port = int(sys.argv[3]) #9999

lines = ssc.socketTextStream(nc, nc_port)

words = lines.flatMap(parse_line)

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.saveAsTextFiles("hdfs:///tmp/irc-data")

#pairs.pprint()

ssc.start()

ssc.awaitTermination()

