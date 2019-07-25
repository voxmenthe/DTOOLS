#!/usr/bin/env bash
#
#
set -e

# Min and max date range for behavorial data
DATE_DAY=${1:-20170911}

echo "Start generating pros_macys_user_profile table"

# Settings for offline data
OUTFILE="pros_macys_user_profile_${DATE_DAY}.tar.gz"

HIVE_SCRIPT="pros_macys_user_profile.hql"

echo "Using ${HIVE_SCRIPT} with date ${DATE_DAY}"

# Get train dataset
hive -f ${HIVE_SCRIPT} \
     -hivevar DATE_DAY=${DATE_DAY};

# Dump final results and transfer

# Rename output files
cat sample_tables/*_0 > pros_macys_user_profile.csv;

# Archive all
tar -zcvf ${OUTFILE} pros_macys_user_profile*.csv ../VERSION

# Cleanup
rm sample_tables/*_0