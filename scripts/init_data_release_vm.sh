#!/bin/bash -ue

# Prepare a fresh VM to run the statistics programs.
# Creates local data directory with watch times, in pwd.

# 1. Clone puffer-statistics/data-release 
git clone https://github.com/StanfordSNR/puffer-statistics.git 
pushd puffer-statistics

# 2. Install dependencies
scripts/deps.sh

# 3. Build 
./autogen.sh
./configure
make -j$(nproc)
sudo make install
popd

# 4. Set constants and paths used below
source scripts/export_constants.sh "$PWD"

# 5. Create local directory for data
mkdir "$LOCAL_DATA_PATH"
cd "$LOCAL_DATA_PATH"

# 6. Download static watch time lists from gs (to be used for all days)
gsutil -q cp "$DATA_BUCKET"/*"$WATCH_TIMES_POSTFIX" .

# 7. (Private only) Set up environment for Postgres connection 
