#!/bin/bash -ue

# Upload day's results to gs (expt settings and CSVs were already uploaded)
cd "$LOCAL_DATA_PATH"/"$END_DATE"

# Upload logs except private err and expt settings
# (TODO: update if private filename changes)
public_logs=$(ls -d logs/* | grep -v "influx" | grep -v "$EXPT")
gsutil cp $public_logs "$DATA_BUCKET"/"$END_DATE"/"$LOGS"

# All private files should be cleaned up by now,
# but enumerate public files just in case
public_results="${STREAM_STATS_PREFIX}* *scheme_stats* *.svg"
gsutil -q cp $public_results "$DATA_BUCKET"/"$END_DATE" # don't quote $public_results -- need the spaces

# Delete local data now that it's in gs, except stream stats
rm -r $(ls | grep -v "${STREAM_STATS_PREFIX}*")
