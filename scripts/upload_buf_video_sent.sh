#!/bin/bash -uex

END_DATE=$1
DATA_BUCKET=gs://puffer-data-release/buf_video_sent/
LOGS=logs
STATS_REPO_PATH=~/puffer-statistics
echo "Starting $END_DATE"

clean_up_err() {
    # Clean up any partial CSVs
    rm -f *.csv
    # Clean up Influx data
    rm -rf "$END_DATE"
    rm -f "$END_DATE".tar.gz
    exit 1
}

# Download Influx export from gs 
/snap/bin/gsutil -q cp gs://puffer-influxdb-analytics/"$END_DATE".tar.gz . # no cleanup needed
tar xf "$END_DATE".tar.gz
# Enter (temporary) data directory
# (pwd now: "$LOCAL_DATA_PATH"/"$END_DATE"/"$END_DATE") 
pushd "$END_DATE" > /dev/null
for f in *.tar.gz; do tar xf "$f"; done
popd > /dev/null

# Influx export => anonymized csv 

# Subshell running influx_to_csv creates this file to signal failure
rm -f influx_to_csv_failed    
readonly influx_to_csv_err="$LOGS"/influx_to_csv_err_"$END_DATE".txt
# If influx_to_csv never starts, tail in subshell consumes export.
# Otherwise, pipeline hangs - note both tail and parens are necessary to avoid hang
influx_inspect export -datadir "$END_DATE" -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
    ("$STATS_REPO_PATH"/influx_to_csv "$END_DATE" 2> "$influx_to_csv_err" || \
    { tail > /dev/null; touch influx_to_csv_failed; }) 

if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    >&2 echo "Error: Influx export failed ($END_DATE)" 
    clean_up_err # exits, in case more functionality is added below
fi 
if [ -f influx_to_csv_failed ]; then
    >&2 echo "Error: Private analyze exited unsuccessfully or never started ($END_DATE)"
    clean_up_err
fi

# Clean up Influx data
rm -r "$END_DATE" 
rm "$END_DATE".tar.gz

# Upload CSVs, zipped (will automatically unzip after download)
if ! /snap/bin/gsutil -q -m cp -Z *$END_DATE.csv "$DATA_BUCKET"; then
    # Clean up any partially uploaded file (ignore exit status)
    /snap/bin/gsutil -q rm "$DATA_BUCKET"*$END_DATE.csv 2> /dev/null || true
    >&2 echo "Error uploading CSVs ($END_DATE)"
    exit 1 
fi

echo "Finished $END_DATE"
