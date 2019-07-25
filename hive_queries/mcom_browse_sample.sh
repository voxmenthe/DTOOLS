#!/usr/bin/env bash
#
#
set -e

echo "Start generating table"

# Settings for offline data
OUTFILE="mcom_browse_sample.tar.gz"

HIVE_SCRIPT="mcom_browse_sample.hql"

echo "Using ${HIVE_SCRIPT}"

# Get train dataset
hive -f ${HIVE_SCRIPT};

# Dump final results and transfer

# Rename output files
cat sample_tables/*_0 > mcom_browse_sample.csv;

# Archive all
tar -zcvf ${OUTFILE} mcom_browse_sample*.csv ../VERSION


