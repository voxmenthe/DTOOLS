#!/usr/bin/env bash
#
#
set -e

echo "Creating trending data temp files in hive"

HIVE_SCRIPT="trending_data.hql"

echo "Using ${HIVE_SCRIPT}"

# Run the script
hive -f ${HIVE_SCRIPT};

hive -e "select * from purchase_cnts LIMIT 500000" > purchase_cnts_500k.csv
hive -e "select * from atb_counts LIMIT 500000" > atb_counts_500k.csv
hive -e "select * from view_counts LIMIT 500000" > view_counts_500k.csv
hive -e "select * from view_atb_purch_counts LIMIT 500000" > view_atb_purch_counts_500k.csv

echo "Done"
