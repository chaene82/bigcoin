#!/bin/bash
# Title:       Restart Twitter Kafka Producer Scripts
# Create-Date: 07. November 2016
# Version:     1.0
# Author:      Cyrill Durrer / Christoph Haene
# Contact:     http://cyrilldurrer.com
#	       christoph.haene@gmail.com
#
# Task:        Restart the twitter producer scripty
#
# Output:      running tasks
#
##############################################################################################################################                                                                                  

######################################################################
# define variables
######################################################################

script='twitter2kafka.py'
path='/home/sparky/Documents/twitter2kafka'
log='twitter2kafka.log'

######################################################################
# kill all running scripts 
######################################################################
pkill -9 -e -f  $script

######################################################################
# start new scripts in background 
######################################################################
/usr/bin/python $path/$script &>> $path/$log &
