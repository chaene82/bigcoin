#!/usr/bin/env python
# Title:       Twitter Stream to Kafka
# Create-Date: 23. Oktober 2016
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       christoph.haene@gmail.com
#
# Task:        Receive Twitter Stream for Bitcoin and send to Kafka 
#
# Output:      Kafka Producer
#
# Kafka:       Create first an topic in Kafka (Path to Kafka: /usr/hdp/2.4.0.0-169/kafka/bin/)
#              kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterstream
##############################################################################################################################                                                                                  

import tweepy
import threading, logging, time
from kafka import KafkaProducer
import string

######################################################################
# Authentication details. To  obtain these visit dev.twitter.com
######################################################################

consumer_key = 'insert here your Twitter Key'
consumer_secret = 'insert here your Twitter Secret'
access_token = 'insert here your Twitter Token'
access_token_secret = 'insert here your Twitter token secret'

mytopic='twitterstream' # Topic should be the same as defined in Kafka

######################################################################
#Create a handler for the streaming data that stays open...
######################################################################

class StdOutListener(tweepy.StreamListener):

    #Handler
    ''' Handles data received from the stream. '''

    ######################################################################
    #For each status event
    ######################################################################

    def on_status(self, status):
        
        # Schema changed to add the tweet text
        print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.text, status.user.screen_name)
        message =  str(status.user.followers_count) + ',' + str(status.user.friends_count) + ',' + str(status.user.statuses_count) + ',' + status.text.replace(",","\,") + ',' + status.user.screen_name + ',' + status.user.lang + ',' + status.user.location.replace(",","\,") + ',' + str(status.created_at)
        msg = filter(lambda x: x in string.printable, message)

        try:
            #write out to kafka topic
            producer.send(mytopic, str(msg))
        except Exception, e:
            return True
        
        return True
       
    ######################################################################
    #Supress Failure to keep script running... 
    ######################################################################
 
    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):

        print('Timeout...')
        return True # To continue listening

######################################################################
# Keep the script running, because of failure NONETYPE is nul() 
######################################################################

def start_stream():
    while True:
        try:
            stream = tweepy.Stream(auth, listener)
            # api = tweepy.API(auth)
            # Stream Twitter Messages with bitcoin or btc
            stream.filter(track=['bitcoin', 'btc'])
        except:
            continue

######################################################################
#Main Loop Init
######################################################################

if __name__ == '__main__':
    
    listener = StdOutListener()

    #sign oath cert

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

    auth.set_access_token(access_token, access_token_secret)

    # Kafka-Producer Konfig
    producer = KafkaProducer(bootstrap_servers='spark1:6667')

    # Start Streaming
    start_stream()


