#!/bin/bash
until python /home/sparky/Documents/kafka_sparks_hbase/wo_calc/kafka_datatype_poloniex.py; do
  echo "Crashed, restarting..." >&2
  sleep 1
done
