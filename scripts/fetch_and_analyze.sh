#!/bin/bash
# For provided date ranges, grabs data from gs and runs analyze 
# Assumed to already be in desired output directory (e.g. called by parallel wrapper)
# Diffs results against the submission (submission_anon_analyze is the version of
# analyze used in the submission, with non-anonymous output suppressed for comparison)
set -e

# For now, private and public in one script
# Export and analyze a single day
single_day_stats() { 
    first_day=$1
    second_day=$(date -I -d "$first_day + 1 day")
    date=${first_day}T11_${second_day}T11 # format 2019-07-01T11_2019-07-02T11
    # echo $date
    # echo "getting data"
    gsutil cp gs://puffer-influxdb-analytics/${date}.tar.gz .
    # untar once to get top-level date containing {manifest, meta, s*.tar} 
    tar xf ${date}.tar.gz
    # untar again on s*.tar.gz to get puffer/retention32d/*/*.tsm 
    # not all s*.tar.gz are nonempty
    pushd ${date}
    for f in *.tar.gz; do tar xf "$f"; done
    popd
    # influx_inspect export -datadir $date -waldir /dev/null -out "influx_out.txt" # useful for test
    # export to influxDB line protocol file
    # pass top-level date to influx_inspect
    # echo "exporting and analyzing"
    
    # Influx export => anonymized csv 
    #echo "starting private analyze" 
    influx_inspect export -datadir $date -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
        ~/puffer-statistics/private_analyze $date 2> ${date}_private_analyze_err.txt 
    #echo "finished private analyze for date " $date
    
    # Anonymized csv => stream-by-stream stats
    cat client_buffer_${date}.csv | ~/puffer-statistics/public_analyze \
        ~/puffer-statistics/experiments/puffer.expt_feb4_2020 $date > ${date}_public_analyze_stats.txt \
        2> ${date}_public_analyze_err.txt
    #echo "finished public analyze"
    
    # Run submission version for comparison
    #echo "starting submission analyze"
    influx_inspect export -datadir $date -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
        ~/puffer-statistics/submission_anon_analyze \
        ~/puffer-statistics/experiments/puffer.expt_feb4_2020 $date > ${date}_submission_anon_stats.txt \
        2> ${date}_submission_anon_err.txt
   
    # clean up data, leave stats/err.txt, csvs
    rm -rf ${date} 
    rm ${date}.tar.gz
    # rm *_${date}.csv

    # diff submission and new stats -- ignore tiny differences from float addition order
    # sort, since public analyze outputs in different order
    stats=(${date}_submission_anon_stats.txt ${date}_public_analyze_stats.txt)
    for stats in "${stats[@]}"; do
        sort -o $stats $stats
        # set up for numdiff
        # sed -i 's/=/= /g' $stats
        # sed -i 's/%//g' $stats
    done
    # numdiff doesn't work with tail
    # numdiff -r 0.005 ${date}_submission_anon_stats.txt ${date}_public_analyze_stats.txt >> diffs.txt
    diff <(tail -n +3 ${date}_submission_anon_stats.txt) <(tail -n +3 ${date}_public_analyze_stats.txt) >> diffs.txt
}

if [ "$#" -lt 1 ]; then
    usage="Provide date ranges; e.g. \`./fetch_and_plot.sh 2019-01-18:2019-08-08 2019-08-29:2019-09-12\` \
           for primary study period 2019-01-18T11_2019-01-19T11 to 2019-08-07T11_2019-08-08T11 \
           and 2019-08-29T11_2019-08-30T11 to 2019-09-11T11_2019-09-12T11 (inclusive)"
    echo $usage
    exit 1
fi

start_time=`date +%s`

for range in "$@"; do
    IFS=':'
    read -ra endpoints <<< "$range"
    cur_date=${endpoints[0]}
    end_date=${endpoints[1]}
    # exclude end (single_day_stats analyzes [cur, cur+1])
    while [ "$cur_date" != "$end_date" ]; do
        single_day_stats $cur_date
        cur_date=$(date -I -d "$cur_date + 1 day")
    done
done

end_time=`date +%s`
runtime=$((end_time-start_time))
echo "runtime, min: " $(($runtime/60))

