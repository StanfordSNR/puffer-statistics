#!/bin/bash -u
# Don't exit or set -e, since script is sourced

# Exports constants needed for initializing VM and entrance program-related scripts.
# If date argument is supplied, END_DATE is exported; else, END_DATE is empty.

USAGE="Usage: export_constants.sh <dir> <date>.\n\
       Dir: Absolute path to directory containing statistics repo and local data.\n\
       Date: Date to be analyzed as 20XX-XX-XX or \"yesterday\". Day starts at 11AM UTC.\n\
             (Optional - not needed during init)\n" 

if [[ "$#" -ne 1 && "$#" -ne 2 ]]; then
    >&2 printf "$USAGE"
    return 1
fi

if [ ! -d "$1"/puffer-statistics ]; then
    >&2 echo "$1 must exist and contain puffer-statistics repo."
    >&2 printf "$USAGE"
    return 1
fi

# Set END_DATE if specified, formatted as 2019-07-01T11_2019-07-02T11
if [ "$#" -eq 2 ]; then
    if [ "$2" == "yesterday" ]; then 
        # current day is postfix of date string;
        # e.g. 2019-07-01T11_2019-07-02T11 backup is at 2019-07-02 11AM UTC
        SECOND_DAY=$(date -I --utc) 
        FIRST_DAY=$(date -I -d "$SECOND_DAY - 1 day")
    else 
        # Check date format
        if ! date -I -d "$2" > /dev/null; then 
            >&2 printf "$USAGE"
            return 1  
        fi
        FIRST_DAY="$2"
        SECOND_DAY=$(date -I -d "$FIRST_DAY + 1 day")
    fi
fi

# Export constants 
set -o allexport
# TODO: these can't be readonly since this script needs to run multiple times 
# in same shell (e.g. once for each date)

# Day to be analyzed (formatted as Influx backup filename)
if [ "$#" -eq 2 ]; then
    END_DATE=${FIRST_DAY}T11_${SECOND_DAY}T11
else
    END_DATE=""
fi
# Absolute path to local dir containing each day's data 
LOCAL_DATA_PATH="$1"/data-release
# Absolute path to local puffer-statistics repo 
STATS_REPO_PATH="$1"/puffer-statistics 
# Directory name for daily metadata, error logs 
# (Full path will be $LOCAL_DATA_PATH/$END_DATE/$LOGS)
LOGS=logs
# Experimental settings dump file
EXPT="$LOGS"/expt_settings
# Google Cloud Storage bucket for data
DATA_BUCKET=gs://puffer-stanford-public/data-release
# Prefix used to identify a stream stats file from any day
STREAM_STATS_PREFIX=stream_stats
# Postfix used to identify watch times files
WATCH_TIMES_POSTFIX=watch_times.txt

set +o allexport
