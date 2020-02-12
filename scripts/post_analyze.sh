#!/bin/bash
# Run schemedays and confint in provided directory containing analyze output; exit if dir does not exist 

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
mkdir results
cd results

# Output of schemedays --build-list, input of schemedays --intersect
scheme_days_file="scheme_days.txt"
# Readable summary of days each scheme has run
scheme_days_summary="scheme_days_summary.txt"

# build scheme days list
cat ../*stats.txt | ~/puffer-statistics/schemedays $scheme_days_file --build-list 2> $scheme_days_summary 
echo "finished schemedays --build-list"

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
    ~/puffer-statistics/schemedays $scheme_days_file --intersect-schemes $schemes --intersect-outfile $intx_out 2> $intx_err
    echo "finished schemedays --intersect"
    
    # run confint using intersection; save output 
    speeds=("all" "slow")  
    for speed in ${speeds[@]}; do
        echo $expt
        echo $speed
        confint_out="${expt}_${speed}_confint_out.txt"
        confint_err="${expt}_${speed}_confint_err.txt"
        plot="${expt}_${speed}_plot.svg"
        d2g="${expt}_${speed}_data-to-gnuplot"
        
        cat ../*stats.txt | ~/puffer-statistics/confinterval --scheme-intersection $intx_out --session-speed $speed > $confint_out 2> $confint_err
        echo "finished confinterval"
        # Useful if there's a version of d2g for each expt/speed
        #cat $confint_out | ~/puffer-statistics/plots/${d2g} | gnuplot > $plot 
    done
done

popd


