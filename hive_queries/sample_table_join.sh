#!/usr/bin/env bash
#
#
set -e

echo "Start generating sample joined table"

# Min and max date range for behavorial data
DATE_DAY=${1:-20170911}

# Settings for offline data
OUTFILE="sample_table_${DATE_DAY}.tar.gz"

HIVE_SCRIPT="sample_table_join.hql"

echo "Using ${HIVE_SCRIPT} with date ${DATE_DAY}"

# Get train dataset
hive -f ${HIVE_SCRIPT} \
     -hivevar DATE_DAY=${DATE_DAY};

# Dump final results and transfer

# Rename output files
cat sample_tables/*_0 > sample_table_join.csv;

# Archive all
tar -zcvf ${OUTFILE} sample_table*.csv ../VERSION