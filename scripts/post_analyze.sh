#!/bin/bash
# Run pre_confinterval and confinterval in provided directory containing analyze output; exit if dir does not exist 

set -e 

if [ "$#" -lt 1 ]; then
    usage="Provide name of directory containing analyze output"
    echo $usage
    exit 1
fi
pushd $1 

if [ ! -d $1 ]; then
    echo "Provided directory does not exist" 
    exit 1
fi

pushd $1
results_dir="results"  
mkdir $results_dir
cd $results_dir

# Output of pre_confinterval --build-schemedays-list, input of pre_confinterval --intersect
scheme_days_out="scheme_days_out.txt"
# Readable summary of days each scheme has run
scheme_days_err="scheme_days_err.txt"
# Output of pre_confinterval --build-watchtimes-list, input of confinterval 
watch_times_out="watch_times_out.txt"
watch_times_err="watch_times_err.txt"

# build scheme days list
cat ../*public_analyze_stats.txt | ~/puffer-statistics/pre_confinterval $scheme_days_out --build-schemedays-list 2> $scheme_days_err 
echo "finished pre_confinterval --build-schemedays-list"
# build watch times lists
cat ../*public_analyze_stats.txt | ~/puffer-statistics/pre_confinterval $watch_times_out --build-watchtimes-list 2> $watch_times_err 
echo "finished pre_confinterval --build-watchtimes-list"

expts=("primary" "vintages" "current")    

for expt in ${expts[@]}; do
    intx_out="${expt}_intx_out.txt"
    intx_err="${expt}_intx_err.txt"
    schemes=$expt
    if [ $expt = "current" ]; then
        schemes="pensieve/bbr,pensieve_in_situ/bbr,puffer_ttp_cl/bbr,linear_bba/bbr"
    fi
    if [ $expt = "emu" ]; then
        schemes="puffer_ttp_emu/bbr,mpc/bbr,robust_mpc/bbr,linear_bba/bbr,puffer_ttp_cl/bbr,pensieve/bbr"
    fi
    if [ $expt = "mle" ]; then
        schemes="puffer_ttp_cl_mle/bbr,puffer_ttp_cl/bbr"
    fi   
    if [ $expt = "linear" ]; then
        schemes="puffer_ttp_linear/bbr,puffer_ttp_cl/bbr"
    fi   
    
    # get intersection using scheme days list
    ~/puffer-statistics/pre_confinterval $scheme_days_out --intersect-schemes $schemes --intersect-out $intx_out 2> $intx_err
    echo "finished pre_confinterval --intersect"
    
    # run confint using intersection; save output 
    speeds=("all" "slow")  
    for speed in ${speeds[@]}; do
        echo $expt
        echo $speed
        confint_out="${expt}_${speed}_confint_out.txt"
        confint_err="${expt}_${speed}_confint_err.txt"
        plot="${expt}_${speed}_plot.svg"
        d2g="${expt}_${speed}_data-to-gnuplot"
        
        cat ../*public_analyze_stats.txt | ~/puffer-statistics/confinterval --scheme-intersection $intx_out \
            --stream-speed $speed --watch-times $watch_times_out > $confint_out 2> $confint_err
        echo "finished confinterval"
        # Useful if there's a version of d2g for each expt/speed
        cat $confint_out | ~/puffer-statistics/plots/${d2g} | gnuplot > $plot 
    done
done

popd


