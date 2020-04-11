#!/bin/bash -ue

# Attempts to fetch all stream statistics files for the month.
# If a file is not found locally in the location expected by entrance program,
# tries to download it from gs (except file corresponding to END_DATE,
# which is the day to be analyzed).
# If gs download fails for any reason, continue to next file. 
# Creates local directory for a day if none exists.

unset period_dates 
MONTH_LEN=30
readarray -t period_dates < <("$STATS_REPO_PATH"/scripts/list_period_dates.sh "$END_DATE" "$MONTH_LEN") 
if [ "${#period_dates[@]}" -ne "$MONTH_LEN" ]; then
    >&2 echo "Error enumerating month dates before fetching month stream stats"
    exit 1
fi

# Make directory for END_DATE, but no need to search for its stream stats -- we're about to generate them
mkdir -p "$LOCAL_DATA_PATH"/"$END_DATE"/"$LOGS"

for period_date in ${period_dates[@]:1}; do
    # Create local directory for day's data if needed
    mkdir -p "$LOCAL_DATA_PATH"/"$period_date"/"$LOGS"
    cd "$LOCAL_DATA_PATH"/"$period_date" 
    stream_stats_filename=${STREAM_STATS_PREFIX}_"$period_date".txt
    
    # could use gsutil cp -n, but would like to avoid talking to gs if not necessary
    if [ ! -f "$stream_stats_filename" ]; then
        # Past day not available locally => try gs 
        if ! gsutil -q cp "$DATA_BUCKET"/"$period_date"/"$stream_stats_filename" . &> /dev/null; then
            # Clean up any partially copied file (stats file didn't exist before, so ok to remove)
            # Conditional overwrites exit status, so we'll continue to next file
            rm -f "$stream_stats_filename" 
        fi  
    fi
done
