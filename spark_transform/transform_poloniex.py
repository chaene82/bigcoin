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

sc = SparkContext(appName="spark_transform")
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
row_start = '1482192000000'   # 26.10.2016
row_end = '1485907199000'     # 31.01.2017
row_day = '604800000'	      # milisec in a day
column_family = "CurrencyRate"
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
   
   hbasekey                    = df.key
   ColNameCurrRateLast	       = "CurrencyRate:last"
   ValueCurrRateLast	       = str(df.Poloniex_last)


   hbase_table_calc.put(str(hbasekey), {ColNameCurrRateLast        : ValueCurrRateLast
                                       })      
   print(str(hbasekey), {ColNameCurrRateLast        : ValueCurrRateLast
                                       })       

#############
# Aggregate data using splitMinutes parameter

def aggregateData(df):

   df_result_twitter = 0
   df_result_polonoiex = 0
   

   #############
   # Poloniex Calculation


      
   df_btc = df.where(df['value'] == 'USDT_BTC').select(df['row'])
   df_joined = df.join(df_btc, ['row'])

   # Calcucate last Price
   df1_poloniex_last = df_joined.where(df['columnFamily'] == 'CurrencyRate').where(df_joined['qualifier'] == 'last').select((df_joined['row'] / (splitMinutes * 60 * 1000)).cast('int').alias('key'), (df_joined['value']).alias('last'))
   df2_poloniex_last = df1_poloniex_last.groupBy('key').agg(avg(col('last')).alias('last'))
   df_poloniex_last = df2_poloniex_last.select((df2_poloniex_last['key'] * (splitMinutes * 60)).alias('key'), (df2_poloniex_last['last']).alias('Poloniex_last'))
   print(df_poloniex_last.show())

   df_result_polonoiex = df_poloniex_last



  
   df_result = df_result_polonoiex

#   print(df_result.show())

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

   if df.count() > 0 :
     if df.where(df['columnFamily'] == 'CurrencyRate').count() > 0:
       df_result = aggregateData(df)

       ######################################################################
       # -- Save records --
       # Loop for all records in the dataframe

       for x in df_result.collect():
         saveRecord(x)
 	
   startRow = str(int(startRow) + int(row_day))
   # End of While Loop
 	


print("### Finish program")
