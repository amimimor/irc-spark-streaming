#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

class Main:
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
   
    @classmethod
    def get_src(line):
       src = lines[2].ltrim("#")
       return src
    
    @classmethod
    def parse_line(s):
       lines = s.split(" ")
       src = self.get_src(lines)
       return lines

import unittest

class MainTester(unittest.TestCase):
   input_example = """:rc-pmtpa!~rc-pmtpa@special.user PRIVMSG #en.wikipedia :14[[07Edward Venables-Vernon-Harcourt14]]4 M10 02https://en.wikipedia.org/w/index.php?diff=699135055&oldid=671587254 5* 03Bender235 5* (+0) 10clean up; http->https (see [[WP:VPR/Archive 127#RfC: Should we convert existing Google and Internet Archive links to HTTPS?|this RfC]]) using [[Project:AWB|AWB]]"""
   lines = input_example.split(" ")

   def parse_test(self):
       m = Main.get_src(line)
       self.assertEqual("#en.wikipedia", m)


if __name__ == '__main__':
    unittest.main()
