#!/bin/bash
# Title:       Restart Poloniex Kafka Producer Scripts
# Create-Date: 07. November 2016
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       christoph.haene@gmail.com
#
# Task:        Restart the poloniex producer scripty
#
# Output:      running tasks
#
##############################################################################################################################                                                                                  

######################################################################
# define variables
######################################################################

script='poloniex2kafka.py'
path='/home/sparky/Documents/poloniex2kafka'
log='poloniex2kafka.log'

######################################################################
# kill all running scripts 
######################################################################
pkill -9 -e -f  $script

######################################################################
# start new scripts in background 
######################################################################
/usr/bin/python3 $path/$script &>> $path/$log &
