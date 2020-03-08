#!/bin/bash
# Installs dependencies for analyze and confinterval, 
# clones and builds puffer-statistics (works for Ubuntu 19.04) 

# add InfluxData repo
wget -qO- https://repos.influxdata.com/influxdb.key | sudo apt-key add -
source /etc/lsb-release
echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list

# get libs
sudo apt-get update
libs=("jemalloc" "jsoncpp" "sparsehash" "boost-all" "crypto++")
for lib in ${libs[@]}; do
    sudo apt-get install -y lib${lib}-dev
done

# get tools 
tools=("influxdb" "gnuplot" "pkg-config") # pkg-config needed for configure
for tool in ${tools[@]}; do
    sudo apt-get install -y $tool
done

# build
git clone https://github.com/StanfordSNR/puffer-statistics.git 

pushd puffer-statistics
./autogen.sh
./configure
make

popd
