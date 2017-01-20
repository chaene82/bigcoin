#!/usr/bin/env python
# Title:       Spark Streaming Job, Kafka to HBase
# Create-Date: 14. Januar 2017
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       	   christoph.haene@gmail.com
#
# Task:        Receive Twitter Messages from Kafka an save it to HBase 
#
# Output:      Into HBase Table
#
##############################################################################################################################   
from __future__ import print_function

import sys
import happybase
import time
import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

timestampeq = 0
hbasekey = 0
counter = 0

def SaveRecord(rdd):    
    global timestampeq
    global hbasekey
    rddarray = rdd.collect()
    if not rddarray:
        return
    for liste in rddarray:
        # Received Kafka Messages will be splittet and transformed to save to HBase
        twittermes = liste[1].encode('utf-8')
        twit = twittermes.replace("\,","###")
        twitterbits = twit.split(",")
        if len(twitterbits) == 8:
            datetimetr = twitterbits[7]
            isdatetime = twitterbits[7].split(":")
            if len(isdatetime) == 3:
                udatetime = datetime.datetime.strptime(str(datetimetr), "%Y-%m-%d %H:%M:%S")
                udatetimets = int(udatetime.strftime("%s"))
            
                twittertext = twitterbits[3].replace("###",",")
                twitterlocation = twitterbits[6].replace("###",",")

				# If more then one message in the same second, count up
                if timestampeq == udatetimets:
                    global counter
                    counter = counter + 1
                else:
                    counter = 0
              
                # HBase Key is timestamp and counter up to 999
                hbasekey = udatetimets * 1000 + counter
                timestampeq = udatetimets
            
                # Write to HBase
                hbase_table_raw.put(str(hbasekey), {'Twitter:followers_count': twitterbits[0], 'Twitter:friends_count': twitterbits[1], \
                'Twitter:status': twitterbits[2], 'Twitter:text': twittertext, 'Twitter:screenname': twitterbits[4], \
                'Twitter:language': twitterbits[5], 'Twitter:location': twitterlocation, 'Twitter:created': twitterbits[7]})
 
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)
    
    # Set Spark Context, timeframe 5 minutes
    sc = SparkContext(appName="TwitterRawData")
    ssc = StreamingContext(sc, 300)

    # Define HBase Thrift Server connection and Table
    hbase_connection = happybase.Connection('192.168.1.10')
    hbase_table_raw = hbase_connection.table('Tablename')

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-twitter", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    # Save Messages to HBase
    kvs.foreachRDD(SaveRecord)

    ssc.start()
    ssc.awaitTermination()
