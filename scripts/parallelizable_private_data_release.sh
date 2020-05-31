#!/bin/bash -ue

source ./export_constants.sh ~ "$1"
./private_entrance.sh 
./public_entrance.sh # TODO: assumes exit after stream_stats (should take arg)
./upload_public_results.sh 
