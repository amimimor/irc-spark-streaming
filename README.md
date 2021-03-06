# Spark Streaming Workshop given by Intel @ BGU, Israel 1/12/2016

In this workshop we'd like to get first hand experience in running a Spark Streaming application. Specifically, we would listen on Wikipedia IRC channel of page edits and find interesting things in it using Spark Streaming and SparkSQL

### Important Notes:
* Spark Streaming using v1.2.0 is not well baked but this is what the cluster offers

* If you see that the streaming app does a lot of retries in connecting to 'nc' than restart both the app and the 'nc'

* When using window operations, don't supply a 'large' interval

* Run using master set to local[*] since 'nc' doesn't handle remote connections well

* Make sure you change to output destination before running (make it parameterized) 

## Exercise 1 - Run a python spark application that performs wordcount on a stream of text
### use https://spark.apache.org/docs/1.2.0/streaming-programming-guide.html 'A Quick Example' to build a working python executable to be run with spark-submit

1. on one terminal:
    `nc -lk PORT`
2. on another terminal:
    `spark-submit streaming_wordcount.py MASTER_PARAM NC_HOST_PARAM NC_PORT_PARAM`
3. type word on the first terminal and enter, switch back to the spark streaming app and see if the counts appear

## Exercise 2 - Spark Streaming Application For Wikipedia IRC Stream
### based on the irc1.py code:
1. count the number of wikipedia edits per country
2. find the max and min edits per country (hint: think what you can do in reduce other than summation)

### based on the irc1.py code, change the ssc batch interval to 1 sec, and use a window function of 10 seconds length, with slide interval of 1 second to:
1. find the total amount of changes to wikipedia during the window
2. find the 'hottest' topic per country
hint: in Spark 1.2.0 window is a bit broken so just use ssc#window

### based on the irc2.py code and the 'process' function, write SQL queries on the stream (!!!) that:
1. tokenize the topics and find the 'hottest' word in the topics per country
2. find how many users have commited more than 100 character changes

