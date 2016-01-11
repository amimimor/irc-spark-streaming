#!/usr/bin/python
"""
run with:
spark-submit ./irc_job2.py local[2] 10.0.0.1 9999

"""
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re

pat_topic = re.compile("\:14\[\[\d+(.*?)14\]\]4")
pattern = r'[\x02\x0F\x16\x1D\x1F]|\x03(\d{,2}(,\d{,2})?)?'

def clean_line(line):
    cleaned = re.sub(pattern, '', line)
    print 'clean: {0}'.format(cleaned)
    return cleaned

def get_src(line):
    src = re.search("(http.*?)\s", line)
    if src:
       return src.group(1)
    else:
       return "i"

def get_topic(line):
    hit = re.search("\[\[(.*?)\]\]", line)
    if hit:
        #print 'I got hit: {0}'.format(hit.group(1))
        return hit.group(1)
    else:
        return ""

def parse_line(s):
       lines = []
       clean = clean_line(s)
       src = get_src(clean)
       topic  = get_topic(clean)
       if src:
         lines.append("!!!!!" + src)
       if topic:
         lines.append("??????" + topic)
       return lines

master = sys.argv[1]

sc = SparkContext(master, "IRC streaming")
ssc = StreamingContext(sc, 10)

# run in another shell:
# ./irc_wiki.sh | grep PRIV | nc -lk 9999

nc = sys.argv[2] #"10.0.0.1"

nc_port = int(sys.argv[3]) #9999

lines = ssc.socketTextStream(nc, nc_port)

words = lines.flatMap(parse_line)

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

words.saveAsTextFiles("hdfs:///tmp/irc-data")

#pairs.pprint()

ssc.start()

ssc.awaitTermination()

