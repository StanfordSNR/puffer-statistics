#!/bin/bash -ue 
# Assumes puffer-statistics and local data live in ~
cd ~/puffer-statistics/scripts
source ./export_constants.sh ~ 2020-04-24 # TODO: test only!
#source ./export_constants.sh ~ yesterday 
echo "sourced"
./private_entrance.sh 
echo "finished private"
./public_entrance.sh 
echo "finished public"
./upload_public_results.sh 
echo "finished upload"
