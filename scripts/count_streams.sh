#!/bin/bash

pushd ~/final_primary_period_only/  
total_num_streams=0
for stats in *stats.txt; do
    # period is 2019-01-26T11_2019-01-27T11:2019-08-07T11_2019-08-08T11, 
    # 2019-08-30T11_2019-08-31T11, and 2019-10-16T11_2019-10-17T11
    num_streams=$(cat $stats | tail -n 2 | head -n 1 | cut -d ' ' -f1 | cut -d '=' -f2)
    # Count *total* streams in primary *period*, regardless of scheme/acceptability
    total_num_streams=$(($total_num_streams + $num_streams))
done
echo "total_num_streams: " $total_num_streams
popd
