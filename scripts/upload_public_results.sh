#!/bin/bash -ue

# Upload day's results to gs (CSVs were already uploaded)
cd "$LOCAL_DATA_PATH"/"$END_DATE"
# Also uploads private_analyze_err, which contains no private info
gsutil -q cp -r "$LOGS" "$DATA_BUCKET"/"$END_DATE"
# All private files should be cleaned up by now,
# but enumerate public files just in case
public_results="${STREAM_STATS_PREFIX}* *scheme_stats* *.svg"
gsutil -q cp $public_results "$DATA_BUCKET"/"$END_DATE" # don't quote $public_results -- need the spaces
