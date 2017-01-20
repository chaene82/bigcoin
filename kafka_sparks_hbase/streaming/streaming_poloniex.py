#!/usr/bin/env python
# Title:       Spark Streaming Job, Kafka to HBase
# Create-Date: 14. Januar 2017
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       	   christoph.haene@gmail.com
#
# Task:        Receive Poloniex Currency Messages from Kafka an save it to HBase 
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
        poloniexmes = liste[1].encode('utf-8')
        poloniexbits = poloniexmes.split(", \'")
        if len(poloniexbits) == 10:
            cpair = poloniexbits[0].split("\'")
            currencypair = cpair[1]

            lastsplit = poloniexbits[1].split("\'")
            last = lastsplit[0]

            lowestasksplit = poloniexbits[2].split("\'")
            lowestask = lowestasksplit[0]

            highestsplit = poloniexbits[3].split("\'")
            highestbid = highestsplit[0]

            percentsplit = poloniexbits[4].split("\'")
            percentchange = percentsplit[0]

            basesplit = poloniexbits[5].split("\'")
            basevolume = highestsplit[0]

            quotesplit = poloniexbits[6].split("\', ")
            quotevolume = quotesplit[0]
            isfrozen = quotesplit[1]

            hightfsplit = poloniexbits[7].split("\'")
            twentyfourhigh = hightfsplit[0]

            lowtfsplit = poloniexbits[8].split("\'")
            twentyfourlow = lowtfsplit[0]

            datetimetr = poloniexbits[9].split("\'")

            udatetime = datetime.datetime.strptime(str(datetimetr[0]), "%Y-%m-%d %H:%M:%S")
            udatetimets = int(udatetime.strftime("%s"))

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
            hbase_table_raw.put(str(hbasekey), {'CurrencyRate:currencypair': currencypair, 'CurrencyRate:last': last, 'CurrencyRate:lowestask': lowestask, 'CurrencyRate:highestbid': highestbid, 'CurrencyRate:percentchange': percentchange, 'CurrencyRate:basevolume': basevolume, 'CurrencyRate:quotevolume': quotevolume, 'CurrencyRate:isfrozen': isfrozen, 'CurrencyRate:24high': twentyfourhigh, 'CurrencyRate:24low': twentyfourlow})

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    # Set Spark Context, timeframe 5 minutes
    scpol = SparkContext(appName="PoloniexRawData")
    sscpol = StreamingContext(scpol, 300)

    # Define HBase Thrift Server connection and Table
    hbase_connection = happybase.Connection('192.168.1.10')
    hbase_table_raw = hbase_connection.table('Tablename')

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(sscpol, zkQuorum, "spark-streaming-poloniex-raw", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    # Save Messages to HBase
    kvs.foreachRDD(SaveRecord)

    sscpol.start()
    sscpol.awaitTermination()
