#!/usr/bin/env python
# Title:       Pythonscript, Kafka to HBase
#              Was used to save older data from Kafka to HBase, not processed by Spark Streaming
# Create-Date: 14. Januar 2017
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       	   christoph.haene@gmail.com
#
# Task:        Receive Messages from Kafka an save it to HBase 
#
# Output:      Into HBase Table
#
##############################################################################################################################   
from kafka import KafkaClient, SimpleConsumer, KafkaConsumer
from sys import argv
import happybase
import time
import datetime

# define HBase Thrift Server and Table
hbase_connection = happybase.Connection('192.168.1.10')
hbase_table_raw = hbase_connection.table('Tablename')

# Kafka Consumer config
consumer = KafkaConsumer("poloniexstream", bootstrap_servers=['spark1:6667'], auto_offset_reset='earliest', enable_auto_commit=False)

counter = 0
hbasekey = 0
timestampeq = 0

# Read saved timestamp
f = open('timestamp.txt','r')
tsff = f.readline()
print(tsff)
f.close()
timestamp_offset = int(tsff)

for message in consumer:

    # Poloniex Example Kafka Output
    # ('BTC_UNITY', '0.00279999', '0.00277131', '0.00266502', '-0.01882117', '1.58188942', '621.30993066', 0, '0.00288008', '0.00250003', '2016-12-01 22:43:39')
    # Received Kafka Messages will be splittet and transformed to save to HBase   
    poloniexmes = message.value
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
            counter = counter + 1
        else:
            counter = 0

        # HBase Key is timestamp and counter up to 999
        hbasekey = udatetimets * 1000 + counter
        timestampeq = udatetimets

        # Define by timestamp when to stop data seeding
        if int(timestampeq) > 1483959600: 
            quit()

        # Write to HBase, but only if timestamp is reached (Skip if older)
        if int(timestampeq) > timestamp_offset:
            # Save reached timestamp all 1000 Messages
            if totalmessages % 1000 == 0:
                print(totalmessages)
                print(timestampeq)
                f = open('timestamp.txt','w')
                f.truncate()
                f.write(str(timestampeq))
                f.close()
            hbase_table_raw.put(str(hbasekey), {'CurrencyRate:currencypair': currencypair, 'CurrencyRate:last': last, 'CurrencyRate:lowestask': lowestask, 'CurrencyRate:highestbid': highestbid, 'CurrencyRate:percentchange': percentchange, 'CurrencyRate:basevolume': basevolume, 'CurrencyRate:quotevolume': quotevolume, 'CurrencyRate:isfrozen': isfrozen, 'CurrencyRate:24high': twentyfourhigh, 'CurrencyRate:24low': twentyfourlow})
