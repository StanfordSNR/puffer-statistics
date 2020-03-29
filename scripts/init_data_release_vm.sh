#!/bin/bash

set -e

# Prepare a fresh VM to run the entrance program.

# Preconditions:
    # Static watch time lists in gs 
    # (For now, uploaded from machine with submission stats;
    # alternative is to analyze full study period on this machine)
    
data_bucket=gs://puffer-stanford-public/data-release-test

# 1. Clone puffer-statistics/data-release 
# (XXX: make repo and data dir configurable; ~ for now)
pushd ~

git clone https://github.com/StanfordSNR/puffer-statistics.git 
pushd puffer-statistics
git checkout data-release   # TODO: this may change

# 2. Install dependencies
scripts/deps.sh

# 3. Build 
./autogen.sh
./configure
make -j$(nproc)
sudo make install
popd

# 4. Create local directory for data
mkdir data-release-test   
pushd data-release-test

# 5. Download static watch time lists from gs (to be used for all days)
gsutil cp "$data_bucket"/*watch_times_out.txt .

# 6. (Private only) Set up environment for Postgres connection 

popd
popd 
