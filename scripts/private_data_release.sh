#!/bin/bash -ue 
# Assumes puffer-statistics and local data live in ~
cd ~/puffer-statistics/scripts
source ./export_constants.sh ~ 2019-01-26
echo "sourced"
./private_entrance.sh 
echo "finished private"
./public_entrance.sh 
echo "finished public"
#./upload_public_results.sh 
echo "finished upload"
