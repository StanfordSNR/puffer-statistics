#!/bin/bash

set -e 

pushd ~/puffer-statistics/post_draft_results
# wtm makes a single file for each scheme -- so if a scheme is in multiple expts, it would combine them...run primary only 
#expts=("primary" "vintages" "current")  # TODO   
#expts=("primary") 
expts=("current") 
#schemes=("puffer_ttp_cl" "mpc" "robust_mpc" "pensieve" "linear_bba")
schemes=("puffer_ttp_cl" "pensieve_in_situ" "pensieve" "linear_bba") # TODO

for expt in ${expts[@]}; do
    echo $expt
    intx_out="${expt}_intx_out.txt"
    
    # assumes confinterval is modified to cout good lines
    # wtm expects a watchtimes directory, writes *.cdf there
    cat ~/final/*stats.txt | ~/puffer-statistics/confinterval --scheme-intersection $intx_out --session-speed all | ~/puffer-statistics/scripts/watch-time-megasessions  
    
    # get means and CI
    for scheme in ${schemes[@]}; do
        pushd watchtimes_current
        stats=$(cat ${scheme}_bbr.cdf | awk '{print $1}' | ministat -n | tail -n 1)
        n=$(echo $stats | cut -d ' ' -f2)
        avg=$(echo $stats | cut -d ' ' -f6)
        stdev=$(echo $stats | cut -d ' ' -f7)
        echo $scheme
        echo $avg "," $n "," $stdev >> stats_current.csv
        popd
    done
done

# *.cdf | wt2.gnu => svg
pushd watchtimes_current
~/puffer-statistics/plots/watchtimes/watchtimes2.gnu > watchtimes_current.svg
popd
popd
