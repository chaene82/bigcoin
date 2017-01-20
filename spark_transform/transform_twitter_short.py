#!/usr/bin/env python
# Title:       Spark tranformation from RAW data to calculated data
# Create-Date: 17. Dezember 2016
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       christoph.haene@gmail.com
#
# Task:        Read data for HBase RAW data to a spark SQL Dataframe
#	       Aggregate data to 5 min  
#
# Output:      HBase Table "BC_CalcData"
# Input:       HBase Table "BC_RawData"
#
#################################################################################################################################       

######################################################################
# Import Libaries
######################################################################

import json
import happybase
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg


######################################################################
# Building Spark SQL Context
######################################################################

sc = SparkContext(appName="spark_transform_short")
sqlContext = SQLContext(sc)


######################################################################
# Happy Base Parameter
######################################################################

hbase_connection = happybase.Connection('192.168.1.114')
hbase_table_calc = hbase_connection.table('BC_CalcData')


######################################################################
# Parameter
######################################################################


hostHbase = 'spark1'
tableInput = 'BC_RawData'
row_start = '1484150399000'   # 26.10.2016
row_end = '1485907199000'     # 31.01.2017
row_day = '86400000'	      # milisec in a day
column_family = "Twitter"
splitMinutes = 5


######################################################################
# Functions
######################################################################

#############
# Read data from hbase by using the newApiHadoopRDD methode for reading to a rdd and transform to a datafram


def readRecord(row_start, row_end):
   conf = {"hbase.zookeeper.quorum": hostHbase, "zookeeper.znode.parent": "/hbase-unsecure", "hbase.mapreduce.inputtable": tableInput, "hbase.mapreduce.scan.row.start": row_start, "hbase.mapreduce.scan.row.stop" : row_end, "hbase.mapreduce.scan.column.family" : column_family}
#conf = {"hbase.zookeeper.quorum": host, "zookeeper.znode.parent": "/hbase-unsecure", "hbase.mapreduce.inputtable": table}
   keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
   valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
   hbase_rdd = sc.newAPIHadoopRDD(
       "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
       "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
       "org.apache.hadoop.hbase.client.Result",
       keyConverter=keyConv,
       valueConverter=valueConv,
       conf=conf)
   hbase_rdd1 = hbase_rdd.flatMapValues(lambda v: v.split("\n"))
   df = sqlContext.jsonRDD(hbase_rdd1.values())
   return df

#############
# Save records to hbase by using happybase (newApiHadoopRDD doesn't work).

def saveRecord(df):  
   #print(type(df))

   # Store Twitter

 
   hbasekey                    = df.key
   ColNameTwitterCount         = "Twitter:Messages"
   ValueTwitterCount           = str(df.Twitter_Count)
   ColNameTwitterCountBTC      = "Twitter:BTC"
   ValueTwitterCountBTC        = str(df.Twitter_Count_BTC)
   ColNameTwitterCountBitcoin  = "Twitter:Bitcoin"
   ValueTwitterCountBitcoin    = str(df.Twitter_Count_BitCoin)

   
      

   hbase_table_calc.put(str(hbasekey),  {ColNameTwitterCount        : ValueTwitterCount,
                                        ColNameTwitterCountBTC     : ValueTwitterCountBTC,
					ColNameTwitterCountBitcoin : ValueTwitterCountBitcoin
                                        })      
   print(str(hbasekey), 		{ColNameTwitterCount        : ValueTwitterCount,
                                        ColNameTwitterCountBTC     : ValueTwitterCountBTC,
					ColNameTwitterCountBitcoin : ValueTwitterCountBitcoin
                                        }) 



#############
# Aggregate data using splitMinutes parameter

def aggregateData(df):

   df_result_twitter = 0
   df_result_polonoiex = 0
   
   #############
   # Twitter Calculation

   

   # Count Twitter Message
   df1_twitter_count = df.where(df['qualifier'] == 'status').select((df['row'] / (splitMinutes * 60 * 1000)).cast('int').alias('key'), df['value'])
   df2_twitter_count = df1_twitter_count.groupBy('key').count()
   df_twitterCount = df2_twitter_count.select((df2_twitter_count['key'] * (splitMinutes * 60)).alias('key'), (df2_twitter_count['count']).alias('Twitter_Count'))

   # Count Twitter BTC Message
   df1_twitter_count_btc = df.where(df['qualifier'] == 'text').filter(col('value').like('%BTC%')).select((df['row'] / (splitMinutes * 60 * 1000)).cast('int').alias('key'), df['value'])
   df2_twitter_count_btc = df1_twitter_count_btc.groupBy('key').count()
   df_twitterCount_btc = df2_twitter_count_btc.select((df2_twitter_count_btc['key'] * (splitMinutes * 60)).alias('key'), (df2_twitter_count_btc['count']).alias('Twitter_Count_BTC'))

   # Count Twitter Bitcoin Message
   df1_twitter_count_bitcoin = df.where(df['qualifier'] == 'text').filter(col('value').like('%Bitcoin%')).select((df['row'] / (splitMinutes * 60 * 1000)).cast('int').alias('key'), df['value'])
   df2_twitter_count_bitcoin = df1_twitter_count_bitcoin.groupBy('key').count()
   df_twitterCount_bitcoin = df2_twitter_count_bitcoin.select((df2_twitter_count_bitcoin['key'] * (splitMinutes * 60)).alias('key'), (df2_twitter_count_bitcoin['count']).alias('Twitter_Count_BitCoin'))

#   df_result_twitter = df_twitterCount.join(df_twitterCount_btc, df_twitterCount.key == df_twitterCount_btc.key, 'inner').join(df_twitterCount_bitcoin, df_twitterCount.key == df_twitterCount_bitcoin.key, 'inner')
   df_result_twitter = df_twitterCount.join(df_twitterCount_btc, ['key']).join(df_twitterCount_bitcoin, ['key'])

   df_result = df_result_twitter


   print(df_result.show())

   return df_result

    


######################################################################
# -- Main Programm --
# Loop day by day for a better performance
######################################################################

print("### Starting program")

startRow = row_start
endRow = row_end

while (startRow < endRow):
   print("Loop for day \n")
   print("Start Row \n")
   print(startRow)


   hbaseStartRow = startRow
   hbaseEndRow = str(int(startRow) + int(row_day) - 1)

   print(hbaseStartRow)
   print(hbaseEndRow)

   df = readRecord(hbaseStartRow, hbaseEndRow)

   print(df.show())


   if df.count() > 0:
      df_result = aggregateData(df)

      ######################################################################
      # -- Save records --
      # Loop for all records in the dataframe

      for x in df_result.collect():
         saveRecord(x)
 	
   startRow = str(int(startRow) + int(row_day))
   # End of While Loop
 	


#print(rdd_result.first())
#print(df.collect())
#print(type(hbase_rdd))
#print(type(df))
#print(df.printSchema())
#print(df_result.show())
print("### Finish program")
