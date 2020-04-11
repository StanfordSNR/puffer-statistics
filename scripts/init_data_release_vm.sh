#!/bin/bash -ue

# Prepare a fresh VM to run the statistics programs.
# Creates local data directory with watch times, in pwd.

# 1. Clone puffer-statistics/data-release 

git clone https://github.com/StanfordSNR/puffer-statistics.git 
pushd puffer-statistics
git checkout data-release   # TODO: this may change

# 2. Install dependencies, set constants and paths used below
scripts/deps.sh
source scripts/export_constants.sh "$PWD"

# 3. Build 
./autogen.sh
./configure
make -j$(nproc)
sudo make install
popd

# 4. Create local directory for data
mkdir "$LOCAL_DATA_PATH"
pushd "$LOCAL_DATA_PATH"

# 5. Download static watch time lists from gs (to be used for all days)
gsutil cp -q "$DATA_BUCKET"/*"$WATCH_TIMES_POSTFIX" .

# 6. (Private only) Set up environment for Postgres connection 

popd
