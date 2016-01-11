# irc-spark-streaming
Spark Streaming Workshop given by Intel at BGU, Isreal

## Exercise 1 - Run a python spark application that performs wordcount on a stream of text
### There is no real problem but getting the application running and seeing it work
1. on one terminal:
    `nc -lk PORT`
2. on another terminal:
    `spark-submit streaming_wordcount.py MASTER_PARAM NC_HOST_PARAM NC_PORT_PARAM`
3. type word on the first terminal and enter, switch back to the spark streaming app and see if the counts appear

## Exercise 2 - Spark Streaming Application For Wikipedia IRC Stream
### based on the irc1.py code:
1. count the number of wikipedia edits per country
2. find the max and min edits per country (hint: think what you can do in reduce other than summation)

### based on the irc1.py code, use a window function of 30 seconds length, with slide interval of 10 seconds to:
1. count the number of wikipedia edits per country
2. find the max and min edits per country



