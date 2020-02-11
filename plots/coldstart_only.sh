#!/bin/bash

set -e 

pushd ~/puffer-statistics/post_draft_results
# wtm makes a single file for each scheme -- so if a scheme is in multiple expts, it would combine them...run primary only 
#expts=("primary" "vintages" "current")  # TODO   
expts=("primary") 
schemes=("puffer_ttp_cl" "mpc" "robust_mpc" "pensieve" "linear_bba")

for expt in ${expts[@]}; do
    echo $expt
    intx_out="${expt}_intx_out.txt"
    
    # assumes confinterval is modified to cout good lines
    # ctm expects a cold-start directory, writes *.cdf there
    cat ~/cold_start/*stats.txt | ~/puffer-statistics/confinterval --scheme-intersection $intx_out --session-speed all | ~/puffer-statistics/scripts/cold-start-megasessions  
    
    # get means and CI (for both startup_delay and first_ssim)
    for scheme in ${schemes[@]}; do
        pushd first_ssims
        stats=$(cat ${scheme}_bbr.cdf | awk '{print $1}' | ministat -n | tail -n 1)
        n=$(echo $stats | cut -d ' ' -f2)
        avg=$(echo $stats | cut -d ' ' -f6)
        stdev=$(echo $stats | cut -d ' ' -f7)
        echo $scheme "," $avg "," $n "," $stdev >> first_ssims.csv  # to be used to create confinterval format for mean/CI
        popd
        pushd startup_delays 
        stats=$(cat ${scheme}_bbr.cdf | awk '{print $1}' | ministat -n | tail -n 1)
        n=$(echo $stats | cut -d ' ' -f2)
        avg=$(echo $stats | cut -d ' ' -f6)
        stdev=$(echo $stats | cut -d ' ' -f7)
        echo $scheme "," $avg "," $n "," $stdev >> startup_delays.csv  # to be used to create confinterval format for mean/CI
        popd
    done
done

exit 0
# plot means and CI (after cold_start_out.txt is created manually from csv data)
# cat ~/puffer-statistics/post_draft_results/cold_start_out.txt | ~/puffer-statistics/plots/cold_start_data-to-gnuplot | gnuplot > cold_start.svg

popd
