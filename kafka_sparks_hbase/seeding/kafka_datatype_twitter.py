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
consumer = KafkaConsumer("twitterstream", bootstrap_servers=['spark1:6667'], auto_offset_reset='earliest', enable_auto_commit=False)

counter = 0
hbasekey = 0
timestampeq = 0

for message in consumer:
            
    # Twitter Example Kafka Output
    # 1887,112,404282,In the last 10 mins\, there were arb opps spanning 13 exchange pair(s)\, yielding profits ranging between $0.01 and $808.76 #bitcoin #btc,ProjectCoin,en,London,2016-12-01 22:46:32
    # Received Kafka Messages will be splittet and transformed to save to HBase                
    twittermes = message.value
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
                counter = counter + 1
            else:
                counter = 0

            # HBase Key is timestamp and counter up to 999                
            hbasekey = udatetimets * 1000 + counter
            timestampeq = udatetimets
            
            # Define by timestamp when to stop data seeding
            if int(timestampeq) > 1483666200:
                quit()

            # Write to HBase, but only if timestamp is reached (Skip if older)
            if int(timestampeq) > 1483576200:
                hbase_table_raw.put(str(hbasekey), {'Twitter:followers_count': twitterbits[0], 'Twitter:friends_count': twitterbits[1], \
        'Twitter:status': twitterbits[2], 'Twitter:text': twittertext, 'Twitter:screenname': twitterbits[4], \
        'Twitter:language': twitterbits[5], 'Twitter:location': twitterlocation, 'Twitter:created': twitterbits[7]})


