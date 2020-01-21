#!/bin/bash
# For provided date ranges, grabs data from gs and generates ./results/plot.svg

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
    # export to influxDB line protocol file
    # pass top-level date to influx_inspect
    # echo "exporting and analyzing"
    influx_inspect export -datadir $date -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
        ~/puffer-statistics/analyze ~/puffer-statistics/experiments/puffer.expt_jan9_2020 > ${date}_stats.txt 2> ${date}_err.txt 

    # clean up data, leave stats/err.txt
    rm -rf ${date}
    rm ${date}.tar.gz
}

if [ "$#" -lt 1 ]; then
    usage="Provide date ranges; e.g. \`./fetch_and_plot.sh 2019-01-18:2019-08-08 2019-08-29:2019-09-12\` \
           for primary study period 2019-01-18T11_2019-01-19T11 to 2019-08-07T11_2019-08-08T11 \
           and 2019-08-29T11_2019-08-30T11 to 2019-09-11T11_2019-09-12T11 (inclusive)"
    echo $usage
    exit 1
fi

start_time=`date +%s`
rm -rf results
mkdir results
pushd results

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

# generate plots from per-day stats
cat *_stats.txt | ~/puffer-statistics/confinterval | ~/puffer-statistics/plots/data-to-gnuplot | gnuplot > plot.svg
end_time=`date +%s`

runtime=$((end_time-start_time))
echo "runtime, min: " $(($runtime/60))

popd
