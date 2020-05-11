#!/bin/bash
# Installs dependencies for analyze, preconfinterval, and confinterval 
# Tested on Ubuntu 19.10
    # Changes for 18.04:
        # jemalloc package may not have a .pc file for pkg-config
        # boost include: container_hash => functional
        # g++8 for charconv
        
# TODO: add matplotlib, python3

# (private only) add InfluxData repo
wget -qO- https://repos.influxdata.com/influxdb.key | sudo apt-key add -
source /etc/lsb-release
echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list

# get libs
sudo apt-get update
libs=("jemalloc" "jsoncpp" "sparsehash" "boost-all" "crypto++")
for lib in ${libs[@]}; do
    sudo apt-get install -y lib${lib}-dev
done

# (private only) for Postgres connect (note PUFFER_PORTAL_DB_KEY is also required)
sudo apt-get install -y libdbd-pg-perl 

# get tools 
tools=("influxdb" "gnuplot" "pkg-config") # pkg-config needed for configure
for tool in ${tools[@]}; do
    sudo apt-get install -y $tool
done

# gsutil is also required, for access to the data bucket
