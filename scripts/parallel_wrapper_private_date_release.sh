#!/bin/bash -ue

# TODO LEFT OFF: source w/ //ization? (OK if all diff shells...)
cd ~/puffer-statistics/scripts

start_time=`date +%s`
# parallel takes space-separated list of args 
args=""
ndays=0

push_args() { 
    cur_date=$1
    end_date=$2
    while [ "$cur_date" != "$end_date" ]; do
        next_date=$(date -I -d "$cur_date + 1 day")
        args=" $args $cur_date"
        cur_date=$next_date
    done
}

# 2019-01-26T11_2019-01-27T11 : 2020-04-14_2020-04-15T11 (inclusive)
#push_args "2019-01-26" "2020-04-15"
push_args "2019-01-26" "2019-01-28" # TODO: gives 2019-01-26 2019-01-27 => goes to 2019-01-27T11_

echo "pid $$"
parallel ~/puffer-statistics/scripts/parallelizable_private_data_release.sh ::: $args

end_time=`date +%s`
runtime=$((end_time-start_time))
echo "wrapper runtime, min: " $(($runtime/60))
