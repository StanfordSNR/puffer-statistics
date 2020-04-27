#!/bin/bash -ue

# Generate and upload desired day's experiment settings and csvs.

clean_up_err() {
    # Clean up any partial CSVs 
    rm -f *.csv 
    # Clean up Influx data
    rm -rf "$END_DATE" 
    rm -f "$END_DATE".tar.gz
    exit 1 
}

# Avoid hang if logs directory not created yet
mkdir -p "$LOCAL_DATA_PATH"/"$END_DATE"/"$LOGS"
cd "$LOCAL_DATA_PATH"/"$END_DATE" 

# 1. Dump and upload expt settings (requires db key to be set)
if ! "$STATS_REPO_PATH"/experiments/dump-puffer-experiment > "$EXPT"; then 
    rm -f "$EXPT" 
    >&2 echo "Error dumping experiment settings ($END_DATE)"
    exit 1 
fi

if ! gsutil -q cp "$EXPT" "$DATA_BUCKET"/"$END_DATE"/"$EXPT"; then
    # Clean up any partially uploaded file (ignore exit status)
    gsutil -q rm "$DATA_BUCKET"/"$END_DATE"/"$EXPT" 2> /dev/null || true
    >&2 echo "Error uploading experiment settings ($END_DATE)"
    exit 1 
fi

# 2. Generate and upload CSVs for desired day

# Download Influx export from gs 
gsutil -q cp gs://puffer-influxdb-analytics/"$END_DATE".tar.gz . # no cleanup needed
tar xf "$END_DATE".tar.gz
# Enter (temporary) data directory
# (pwd now: "$LOCAL_DATA_PATH"/"$END_DATE"/"$END_DATE") 
pushd "$END_DATE" > /dev/null
for f in *.tar.gz; do tar xf "$f"; done
popd > /dev/null

# Influx export => anonymized csv 

# Subshell running private_analyze creates this file to signal failure
rm -f private_analyze_failed    
readonly private_analyze_err="$LOGS"/private_analyze_err_"$END_DATE".txt
# If private_analyze never starts, tail in subshell consumes export.
# Otherwise, pipeline hangs - note both tail and parens are necessary to avoid hang
influx_inspect export -datadir "$END_DATE" -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
    ("$STATS_REPO_PATH"/private_analyze "$END_DATE" 2> "$private_analyze_err" || \
    { tail > /dev/null; touch private_analyze_failed; }) 

if [ "${PIPESTATUS[0]}" -ne 0 ]; then
    >&2 echo "Error: Influx export failed ($END_DATE)" 
    clean_up_err # exits, in case more functionality is added below
fi 
if [ -f private_analyze_failed ]; then
    >&2 echo "Error: Private analyze exited unsuccessfully or never started ($END_DATE)"
    clean_up_err
fi

# Clean up Influx data
rm -r "$END_DATE" 
rm "$END_DATE".tar.gz

# Upload CSVs, zipped (will automatically unzip after download)
if ! gsutil -q -m cp -Z *.csv "$DATA_BUCKET"/"$END_DATE"; then
    # Clean up any partially uploaded file (ignore exit status)
    gsutil -q rm "$DATA_BUCKET"/"$END_DATE"/*.csv 2> /dev/null || true
    >&2 echo "Error uploading CSVs ($END_DATE)"
    exit 1 
fi
