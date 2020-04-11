#!/bin/bash -ue

# Create local directory for day's data if needed
mkdir -p "$LOCAL_DATA_PATH"/"$END_DATE" 
cd "$LOCAL_DATA_PATH"/"$END_DATE" 

# Download experiment settings and CSVs from gs
gsutil -q cp "$DATA_BUCKET"/"$END_DATE"/"$EXPT" "$LOGS"
# (Analysis only requires client_buffer and video_sent CSVs,
# so public could just download those)
gsutil -q cp "$DATA_BUCKET"/"$END_DATE"/*.csv .
