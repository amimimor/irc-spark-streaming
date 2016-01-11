#!/usr/bin/python
"""
run with:
spark-submit ./irc2.py local[2] 10.0.0.1 9999

"""

import sys
# if you are running in standalone and want to run as a __main__ or unittest
# you need to add SPARK_HOME to PYTHONPATH so that pyspark module
# would be available
# you'd also need to do 'pip install py4j'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
import re
"""
Helper functions to parsing the IRC input lines
Could be migrated nicely to another file and loaded as module
that could be specified by spark-submit with the '--py-files' param
"""
pat_topic = re.compile("\:14\[\[\d+(.*?)14\]\]4")
pattern = r'[\x02\x0F\x16\x1D\x1F]|\x03(\d{,2}(,\d{,2})?)?'

# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def clean_line(line):
    cleaned = re.sub(pattern, '', line)
    #print 'clean: {0}'.format(cleaned)
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
    m = re.search("\]\]\s(.+)\shttp", line)
    if m:
       return m.group(1)
    return ""

def get_user(line):
    user = re.search("\*\s(.*?)\s\*", line)
    if user:
       return user.group(1)
    return ""

def get_delta(line):
    delta = re.search("\s\(([\-\+]\d+)\)\s", line)
    if delta:
       return delta.group(1)
    return 0

def parse_line(s):
    clean = clean_line(s)
    url = get_url(clean)
    topic  = get_topic(clean)
    cha = get_irc_channel(clean)
    m = get_mod_flag(clean)
    user = get_user(clean)
    delta = int(get_delta(clean))
    d = {'url': url, 'topic': topic, 'cha': cha, 'm': m, 'user': user, 'delta': delta}
    return d


def process(rdd):
    print '=========  ========='
    try:
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)
        # Convert RDD[Dict] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(**w))
        wordsDataFrame = sqlContext.inferSchema(rowRdd)
        # Register as table
        wordsDataFrame.registerTempTable("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = sqlContext.sql("select topic, count(*) as total from words group by topic")
        return wordCountsDataFrame
    except Exception, e:
        print 'Runtime unhandled excption: {0}'.format(e)

"""
Pyspark Flow
"""
master = sys.argv[1]
# create the SparkContext with 'master' which is build using a user param
sc = SparkContext(master, "IRC SQL Streaming")
# crate a Streaming Spark Context with 'batch interval' of 10 seconds
ssc = StreamingContext(sc, 10)

# run in another shell:
# ./irc_wiki.sh | grep PRIV | nc -lk 9999

# specify using user param the address of the nc server
nc = sys.argv[2] #"10.0.0.1"
# specify using user param the port of the nc server
nc_port = int(sys.argv[3]) #9999
# read from socket as a stream of text.
# lines is type DStream[string]
lines = ssc.socketTextStream(nc, nc_port)
# operate on each line and map it to a local function 'parse_line' that creates a dictionary
words = lines.map(parse_line)

counts = words.transform(process)
# save the dictionary to hdfs by specifing a directory name perfix
counts.saveAsTextFiles("hdfs:///tmp/sql")
# start the streaming
ssc.start()
# tell spark to stop the application upon user termination (ctrl-c)
ssc.awaitTermination()

