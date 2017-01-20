#!/usr/bin/env python
# Title:       Poloniex Stream to Kafka
# Create-Date: 23. Oktober 2016
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       christoph.haene@gmail.com
#
# Task:        Receive Poloniex Stream for Bitcoin and send to Kafka 
#
# Output:      Kafka Producer
#
# Kafka:       Create first an topic in Kafka (Path to Kafka: /usr/hdp/2.4.0.0-169/kafka/bin/)
#              kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic poloniexstream
#              kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic poloniextrollbox
#################################################################################################################################                                                                                  

from kafka import KafkaProducer
from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.wamp import ApplicationRunner
from asyncio import coroutine
import string
import datetime

mytopic='poloniexstream'# Topic should be the same as defined in Kafka
mytopic2='poloniextrollbox'

######################################################################
#Create a handler for the streaming data that stays open...
######################################################################
            

class PoloniexComponent(ApplicationSession):
    def onConnect(self):
        self.join(self.config.realm)

    @coroutine
    def onJoin(self, details):
        def onTicker(*args):
            print("Ticker event received:", args)
            timest = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            args = args + (timest, )
            msg = str(args)
            producer.send(mytopic, bytes(msg, 'utf-8'))

        def onTicker2(*args):
            print("Trollbox received:", args)
            timest = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            args = args + (timest, )
            msg = str(args)
            producer.send(mytopic2, bytes(msg, 'utf-8'))

        try:
            yield from self.subscribe(onTicker, 'ticker')
            yield from self.subscribe(onTicker2, 'trollbox')
        except Exception as e:
            print("Could not subscribe to topic:", e)

def start_poloniex():
    while True:
        try:
            runner = ApplicationRunner("wss://api.poloniex.com:443", "realm1")
            runner.run(PoloniexComponent)
        except:
            continue
######################################################################
#Main Loop Init
######################################################################


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='spark1:6667')
    start_poloniex()


