#!/bin/bash
set -e

# For desired day:
    # Generate csvs and per-STREAM stats. 
    # Desired day is optional argument to entrance program (format e.g. 2019-07-01T11_2019-07-02T11).
        # Defaults to current day (UTC)
# For desired day/week/two weeks/month (if data available on local or gs):
    # Generate per-SCHEME stats and plots.  
    # (Note this introduces a dependency across days, so be careful parallelizing)
# Upload public data to gs.

# Preconditions: 
    # Dependencies installed
    # On data-release branch of puffer-statistics (TODO move to master)
    # Local_data_path already exists, but does not contain directory for desired day
    # Postgres key is set
    # Desired day's Influx data is in puffer-influxdb-analytics bucket
    # Static watch time lists in gs shared among all days? (TODO)
        # TODO: add check to confint for number of watch_times -- how many is enough?
        # Note: To build watch times lists:
            # (Input data should include at least a year or so,
            # since we sample from these lists to calculate confidence intervals)
            # watch_times_err="watch_times_err.txt"
            # cat ../*/*public_analyze_stats.txt | "$stats_repo_path"/pre_confinterval \
            #    "$watch_times_out" --build-watchtimes-list 2> "$watch_times_err" 

main() { 
    # Absolute path to puffer-statistics repo 
    stats_repo_path=~/puffer-statistics 
    # Absolute path to local dir containing each day's data 
    local_data_path=~/data-release-test               
    # Experimental settings dump
    expt="expt_settings"
    # Google Cloud Storage bucket for data
    # TODO: remove "test" from bucket and data dir names
    data_bucket=gs://puffer-stanford-public/data-release-test
    
    # List of all days each scheme has run
    # Note: confinterval ignores days outside the desired time period,
    # so all time periods can use the same scheme_days_out
    # (time period only needs to be a subset of scheme_days_out)
    # P.S. Doing time period filtering in confinterval requires confinterval to read in more stats
    # than necessary, so if current dir has a ton of stats,
    # could organize into subdirectories and only input the relevant ones
    scheme_days_out="scheme_days_out.txt"
    # Readable summary of scheme_days_out
    scheme_days_err="scheme_days_err.txt"
    
    # List of watch times (should already be in root of local data dir)
    watch_times_out="watch_times_out.txt"
    
    # List of days all requested schemes were run
    intx_out="intx_out.txt"
    # Just a log
    intx_err="intx_err.txt"

    # Get date, formatted as 2019-07-01T11_2019-07-02T11
    if [ "$#" -eq 1 ]; then
        # TODO: check arg format
        date="$1"
    else 
        # current day is postfix of date string;
        # e.g. 2019-07-01T11_2019-07-02T11 backup is at 2019-07-02 11AM UTC
        local second_day=$(date -I --utc) 
        local first_day=$(date -I -d "$second_day - 1 day")
        date=${first_day}T11_${second_day}T11 
    fi

    # First day in desired date; e.g. 2019-07-01 for 2019-07-01T11_2019-07-02T11
    date_prefix=${date:0:10}

    if [ -d "$local_data_path"/"$date" ]; then
        echo "Local directory" "$local_data_path"/"$date" "already exists" 
        exit 1
    fi

    # Create local directory for day's data 
    # (corresponding gs directory is created during upload)
    mkdir "$local_data_path"/"$date" 
    pushd "$local_data_path"/"$date"

    # Dump expt settings to local (will be uploaded to gs with the other data)
    # Private only; for public, replace with download from gs
    # Requires db key to be set
    "$stats_repo_path"/experiments/dump-puffer-experiment > "$expt" 
    
    # Generate STREAM stats for desired day 
    single_day_stats 
    
    # Prerequisites for per-scheme analysis
    run_pre_confinterval
    
    # Generate SCHEME stats/plots for desired day, week, two weeks, month 
    run_confinterval

    # Upload day's PUBLIC data to gs (private only)
    # TODO: which files exactly do we want? 
    # TODO: should public version of entrance be a separate file? 
    to_upload=$(ls | grep -v *private*)
    gsutil cp $to_upload "$data_bucket"/"$date" # don't quote $to_upload -- need the spaces
    
    popd
}

single_day_stats() { 
    # 1. Download Influx export from gs (private only)
    gsutil cp gs://puffer-influxdb-analytics/"$date".tar.gz . # exits if not found  
    tar xf "$date".tar.gz
    # Enter (temporary) data directory
    # (pwd now: "$local_data_path"/"$date"/"$date") 
    pushd "$date"
    for f in *.tar.gz; do tar xf "$f"; done
    popd
    
    # 2. Influx export => anonymized csv (private only)
    # TODO: abort script if right side of pipe errors
    influx_inspect export -datadir "$date" -waldir /dev/null -out /dev/fd/3 3>&1 1>/dev/null | \
        "$stats_repo_path"/private_analyze "$date" 2> "$date"_private_analyze_err.txt 
    
    # 3. Anonymized csv => stream-by-stream stats
    # TODO: Do we want date in (any of) the filenames, if both local and gs have a dir for each date?
    cat client_buffer_"$date".csv | "$stats_repo_path"/public_analyze \
        "$expt" "$date" > "$date"_public_analyze_stats.txt 2> "$date"_public_analyze_err.txt
            
    # Clean up data, leave stats/err.txt, csvs
    rm -rf "$date" 
    rm "$date".tar.gz
}

run_pre_confinterval() {
    # 1. Determine which schemes ran on the desired day
    desired_schemes="desired_schemes.txt" # Desired day's list of schemes
    
    # pre_confinterval outputs each scheme's name as well as the days it ran
    # (Note this is dependent on the format of the output)
    cat "$date"_public_analyze_stats.txt | "$stats_repo_path"/pre_confinterval \
        "$desired_schemes" --build-schemedays-list 2> "$scheme_days_err"    
    
    # Convert to comma-separated list 
    schemes=$(cat "$desired_schemes" | cut -d ' ' -f1 | tr '\n' ',')
    schemes=${schemes%?} # remove trailing comma
    # Only needed to get desired schemes
    rm "$desired_schemes"

    # 2. Build scheme days list 
    # (input data must include all days to be plotted)
    cat ../*/*public_analyze_stats.txt | "$stats_repo_path"/pre_confinterval \
        "$scheme_days_out" --build-schemedays-list 2>> "$scheme_days_err"
        # append to err from build-schemedays-list above

    # 3. Get intersection using scheme days list
    # Note: confinterval ignores a day's data if the day is not in this list 
    # (meaning not all schemes ran that day)
    "$stats_repo_path"/pre_confinterval \
        "$scheme_days_out" --intersect-schemes "$schemes" --intersect-out "$intx_out" 2> "$intx_err"
}

# Given length of period, return first day in period
# formatted as 2019-07-01T11_2019-07-02T11
# e.g. date=2019-07-01T11_2019-07-02T11, length=2 => 2019-06-30T11_2019-07-01T11
time_period_start() {
    let n_days_to_subtract="$1 - 1" 
    local first_day_prefix=$(date -I -d "$date_prefix - $n_days_to_subtract days")
    local first_day_postfix=$(date -I -d "$first_day_prefix + 1 day")
    # use echo to return result, so this function should not echo anything else 
    echo ${first_day_prefix}T11_${first_day_postfix}T11 
}

# $1: number of days in time period (e.g. 7 for week)
# Check if all data is available (either locally or in gs) for the period.
# If data is in gs but not local, download it.
period_avail() {
    local avail=true
    local ndays=$1
    # Day before desired date forms a time period of length 2
    for ((i = 2; i <= $ndays; i++)); do
        local past_date=$(time_period_start $i)
        if [ ! -d "$local_data_path"/"$past_date" ]; then 
            # Past day not available locally => try gs
            # gsutil stat requires /* to be appended
            local avail_in_gs=$(gsutil -q stat "$data_bucket"/"$past_date"/*; echo "$?")
            if [ "$avail_in_gs" -eq 1 ]; then 
                # stat returns 1 if not found
                avail=false
                break
            else
                # Past day is available in gs => download it
                gsutil cp -r "$data_bucket"/"$past_date" "$local_data_path"
            fi
        fi
    done
    # use echo to return result, so this function should not echo anything else 
    echo "$avail"
}

run_confinterval() {
    # TODO: all speeds only for now 
    
    declare -A time_periods # days per period
    time_periods[day]=1 
    time_periods[week]=7 
    time_periods[two_weeks]=14 
    time_periods[month]=30 
    
    # For each time_period with enough data: Calculate confidence intervals and plot
    for time_period in ${!time_periods[@]}; do
        # Skip time period if not enough data available
        # (Currently confint doesn't check if input data contains full range passed to --days.
        # It could, but outfiles would still be created which could be confusing)
        local ndays=${time_periods[$time_period]}
        local avail=$(period_avail "$ndays") 
        if [ "$avail" == "false" ]; then
            echo "Skipping time period" $time_period "with insufficient data available"
            continue
        fi

        local confint_out="${time_period}_confint_out.txt"
        local confint_err="${time_period}_confint_err.txt"
        local date_range=$(time_period_start "$ndays"):"$date"

        # OK if local has stats out of desired range (either older or newer);
        # confint filters on range
        cat ../*/*public_analyze_stats.txt | "$stats_repo_path"/confinterval \
            --scheme-intersection "$intx_out" --stream-speed all \
            --watch-times ../"$watch_times_out" \
            --days "$date_range" > "$confint_out" 2> "$confint_err"

        local d2g="ssim_stall_to_gnuplot" # axes adjust automatically
        local plot="${time_period}_plot.svg"
        # TODO: title plots
        cat "$confint_out" | "$stats_repo_path"/plots/${d2g} | gnuplot > "$plot" 
    done
}

main "$@"

