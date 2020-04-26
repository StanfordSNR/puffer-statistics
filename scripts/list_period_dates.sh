#!/bin/bash -ue

# Output all stream statistics dates corresponding to a time period 
# (in decreasing order, starting with <date>). 
# Date format e.g. 2019-07-01T11_2019-07-02T11
USAGE="./list_period_dates.sh <date> <ndays>, where period is <ndays>, ending on and including <date>"

if [ "$#" -ne 2 ]; then
    echo $USAGE
    return 1
fi
readonly period_end_date=$1
readonly ndays=$2
# First day in desired date; e.g. 2019-07-01 for 2019-07-01T11_2019-07-02T11
readonly period_end_date_prefix=${period_end_date:0:10}

for ((i = 0; i < $ndays; i++)); do
    period_day_prefix=$(date -I -d "$period_end_date_prefix - $i days")
    period_day_postfix=$(date -I -d "$period_day_prefix + 1 day")
    # use echo to return result, so this script should not echo anything else 
    echo ${period_day_prefix}T11_${period_day_postfix}T11 
done
