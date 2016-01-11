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

def get_url(line):
    url = re.search("(http.*?)\s", line)
    if url:
       return url.group(1)
    else:
       return ""

def get_topic(line):
    hit = re.search("\[\[(.*?)\]\]", line)
    if hit:
        #print 'I got hit: {0}'.format(hit.group(1))
        return hit.group(1)
    return ""

def get_irc_channel(line):
    cha = re.search("\#(\w+)\.wikipedia", line)
    if cha:
       return cha.group(1)
    return ""

def get_mod_flag(line):
    m = re.search("(.)\shttp", line)
    if m:
       return m.group(1)
    return ""

def get_user(line):
    user = re.search("\*\s(.*?)\s\*", line)
    if user:
       return user.group(1)
    return ""

def parse_line(s):
       lines = []
       clean = clean_line(s)
       url = get_url(clean)
       topic  = get_topic(clean)
       cha = get_irc_channel(clean)
       m = get_mod_flag(clean)
       user = get_user(clean)
       if url:
         lines.append('url: {0}'.format(url))
       if topic:
         lines.append('topic: {0}'.format(topic))
       if cha:
         lines.append('channel: {0}'.format(cha))
       if m:
         lines.append('m: {0}'.format(m))
       if user:
         lines.append('user: {0}'.format(user))
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

