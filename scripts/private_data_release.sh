#!/bin/bash -ue 
# Assumes puffer-statistics and local data live in ~
cd ~/puffer-statistics/scripts
source ./export_constants.sh ~ yesterday
echo "sourced"
./get_month_stream_stats.sh 
echo "got month stats"
./private_entrance.sh 
echo "finished private"
./public_entrance.sh 
echo "finished public"
./upload_public_results.sh 
