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
consumer = KafkaConsumer("poloniextrollbox", bootstrap_servers=['spark1:6667'], auto_offset_reset='earliest', enable_auto_commit=False)

counter = 0
hbasekey = 0
timestampeq = 0

for message in consumer:

    # Trollbox Example Kafka Output
    # ('trollboxMessage', 11717292, 'daedalus', 'How did you guys learn to understand candlesticks', 0, '2016-11-21 23:56:21')
    # Received Kafka Messages will be splittet and transformed to save to HBase
    trollmes = message.value
    trollbits = trollmes.split(", \'")
    if len(trollbits) == 4:
        troll = trollbits[1].split("\'")
        trollmessage = trollbits[2].split("\'")
        datetimetr = trollbits[3].split("\'")
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
        if int(timestampeq) > 1483666200:
            quit()

        # Write to HBase, but only if timestamp is reached (Skip if older)
        if int(timestampeq) > 1483576200:   
            hbase_table_raw.put(str(hbasekey), {'Trollbox:troll': troll[0], 'Trollbox:trollmessage': trollmessage[0]})


