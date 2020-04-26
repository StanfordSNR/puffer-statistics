#!/bin/bash -ue

# Attempts to fetch all stream statistics files for the period.
# If a file is not found locally in the location expected by entrance program,
# tries to download it from gs.
# If gs download fails for any reason, continue to next file. 
# Creates local directory for a day if none exists.

# Date format e.g. 2019-07-01T11_2019-07-02T11
USAGE="$0 <date> <ndays>, where period is <ndays>, ending on and including <date>"

if [ "$#" -ne 2 ]; then
    >&2 echo $USAGE
    exit 1
fi

unset period_dates 
readarray -t period_dates < <("$STATS_REPO_PATH"/scripts/list_period_dates.sh "$1" "$2") 
if [ "${#period_dates[@]}" -ne "$2" ]; then
    >&2 echo "Error enumerating dates before fetching period stream stats"
    exit 1
fi

# Search for END_DATE stream stats as well; in the normal case we're about to generate them,
# but if this is being run after they were already uploaded from another machine,
# then we'd probably like to fetch them 
for period_date in ${period_dates[@]}; do
    # Create local directory for day's data if needed
    mkdir -p "$LOCAL_DATA_PATH"/"$period_date"/"$LOGS" 
    
    cd "$LOCAL_DATA_PATH"/"$period_date" 
    stream_stats_filename=${STREAM_STATS_PREFIX}_"$period_date".txt
    
    # could use gsutil cp -n, but would like to avoid talking to gs if not necessary
    if [ ! -f "$stream_stats_filename" ]; then
        # Past day not available locally => try gs, fail silently (not an error if file not found) 
        if ! gsutil -q cp "$DATA_BUCKET"/"$period_date"/"$stream_stats_filename" . &> /dev/null; then
            # Clean up any partially copied file (stats file didn't exist before, so ok to remove)
            # Conditional overwrites exit status, so we'll continue to next file
            rm -f "$stream_stats_filename" 
        fi  
    fi
done
