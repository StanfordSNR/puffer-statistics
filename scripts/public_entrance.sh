#!/bin/bash -ue

# For desired day: generate per-STREAM stats.
# For desired day/week/two weeks/month (if prereq files and data available locally):
    # Generate per-SCHEME stats and plots.  
    # (Note this introduces a dependency across days, so be careful parallelizing)

main() {
    # List of contiguous days on which all requested schemes ran, ending in END_DATE.
    # Note: All time periods can use the same scheme intersection
    # (time period only needs to be a subset of scheme intersection).
    # The stream stats input to confinterval contain exactly the desired time period,
    # so any days in scheme schedule outside the period are not considered when calculating scheme stats.
    readonly duration_scheme_intersection="$LOGS"/duration_scheme_intersection_"$END_DATE".txt
    
    # Avoid hang if logs directory not created yet/deleted
    mkdir -p "$LOCAL_DATA_PATH"/"$END_DATE"/"$LOGS" 
    cd "$LOCAL_DATA_PATH"/"$END_DATE" 
    readonly end_date_prefix=${END_DATE:0:10}
    
    readonly CHUNK_LEN=30
    # 1. Generate STREAM stats for desired day 
    stream_stats
    
    # 2. Prerequisites for per-scheme analysis
    run_pre_confinterval

    # 3. Generate SCHEME stats/plots for desired day, week, two weeks, month 
    scheme_stats
}

stream_stats() {
    readonly public_analyze_err="$LOGS"/public_analyze_err_"$END_DATE".txt
    # Anonymized CSVs => stream-by-stream stats
    # public_analyze opens the CSVs itself
    if ! "$STATS_REPO_PATH"/public_analyze \
             "$EXPT" "$END_DATE" \
             > ${STREAM_STATS_PREFIX}_"$END_DATE".txt \
             2> "$public_analyze_err"; then
        
        # Clean up any partial stream stats
        rm -f ${STREAM_STATS_PREFIX}_"$END_DATE".txt
       
        >&2 echo "Error generating stream stats from CSVs ($END_DATE)"
        exit 1
    fi
}

# Return a list of filenames representing the CONTIGUOUS subset of the scheme intersection,
# i.e. the subset during which all schemes that ran on the last day, ran every day.
# (Period is specified by $ndays and the last day of the scheme intersection i.e. END_DATE)
# List is in reverse date order.
# Note this starts at the end of the intersection on every call, so will catch holes between 
# chunks as well as holes within a chunk (a hole results from stopping and then restarting an expt).
list_contiguous_filenames() {
    # 1. Read scheme intersection file into convenient format 
    unset scheme_intx_ts
    readarray -t -d ' ' scheme_intx_ts < <(tail "$duration_scheme_intersection" -n +2)
    # intersection ts are in increasing order
    unset scheme_intx_ts[${#scheme_intx_ts[@]}-1] # strip empty element
    
    local old_prefix=${END_DATE:14:10}
    # 2. Working backward from end of intersection, stop after the first non-contiguous day (if any)
    # (no need to check if stream stats file is available -- if it's in the intersection, it has to be)
    for (( i = ${#scheme_intx_ts[@]} - 1; i >= 0; i-- )) ; do
        local ts=${scheme_intx_ts[$i]} 
        # 11am ts => date string e.g. 2020-02-02T11_2020-02-03T11
        local intx_date_prefix=$(date -I --date=@$ts --utc)
        local intx_date_postfix=$(date -I -d "$intx_date_prefix + 1 day")
        intx_date=${intx_date_prefix}T11_${intx_date_postfix}T11

        # Check contiguous
        if [ "$intx_date_postfix" != "$old_prefix" ]; then
            # Contiguous iff new postfix == old prefix (working backward)
            >&2 echo "$end_date_prefix discovered hole on: $intx_date" 
            return # Not an error -- just found the end of a contiguous period
        fi 

        # Date is ok -- "return" it
        local stream_stats_file="$LOCAL_DATA_PATH"/"$intx_date"/${STREAM_STATS_PREFIX}_"$intx_date".txt
        echo "$stream_stats_file"
        old_prefix="$intx_date_prefix"
    done
}

# Write scheme_intersection over the period of length ndays, ending in and including END_DATE.
# This may involve downloading stream stats.
get_period_intersection() {
    local ndays="$1"
    local schemes="$2"
    
    # 1. Download any unavailable stream stats in this chunk
    # Number of days already intersected before this call (period length - chunk length)
    let "nknown_days=ndays - CHUNK_LEN" || true
    local last_chunk_day=$(date -I -d "$end_date_prefix - $nknown_days days")
    "$STATS_REPO_PATH"/scripts/get_period_stream_stats.sh $last_chunk_day $CHUNK_LEN

    # 2. Get filenames of stream stats over the period
    # 2a. Get dates
    unset period_dates
    readarray -t period_dates < <("$STATS_REPO_PATH"/scripts/list_period_dates.sh "$END_DATE" "$ndays") 
    if [ "${#period_dates[@]}" -ne "$ndays" ]; then
        >&2 echo "Error enumerating month dates while getting scheme intersection ($END_DATE, $ndays days)"
        exit 1
    fi

    # 2b. Map dates to filenames, for convenience
    declare -a period_stream_stats_files
    for i in ${!period_dates[@]}; do
        period_date="${period_dates[$i]}"
        stream_stats_file="$LOCAL_DATA_PATH"/"$period_date"/${STREAM_STATS_PREFIX}_"$period_date".txt
        period_stream_stats_files[$i]="$stream_stats_file"
    done

    # Scheme schedule over the entire period
    local duration_scheme_schedule="$LOGS"/duration_scheme_schedule_"$END_DATE".txt
    # Readable summary of scheme schedule (and corresponding stderr)
    local pretty_duration_scheme_schedule="$LOGS"/pretty_duration_scheme_schedule_"$END_DATE".txt

    # 3. Build scheme schedule over the entire period
    set +o pipefail # ignore errors from missing files
    if ! cat "${period_stream_stats_files[@]}" 2> /dev/null |
            "$STATS_REPO_PATH"/pre_confinterval \
            "$duration_scheme_schedule" --build-schemedays-list \
            2> "$pretty_duration_scheme_schedule"; then 
             
        >&2 echo "Error building scheme schedule ($1); pre_confinterval exited unsuccessfully 
                  or never started (see $pretty_duration_scheme_schedule)"
        rm -f "$duration_scheme_schedule"
        exit 1
    fi

    # 4. Get intersection over the entire period, using scheme schedule
    # Note: confinterval ignores a day's data if the day is not in this list 
    # (meaning not all schemes ran that day)
    local intx_err="$LOGS"/intersection_log.txt

    if ! "$STATS_REPO_PATH"/pre_confinterval \
            "$duration_scheme_schedule" --intersect-schemes "$schemes" \
            --intersect-out "$duration_scheme_intersection" \
            2> "$intx_err"; then

        >&2 echo "Error generating scheme intersection ($1); pre_confinterval exited unsuccessfully
                  or never started (see $intx_err)" 
        rm -f "$duration_scheme_intersection" 
        exit 1
    fi
}

run_pre_confinterval() {    
    # Desired day's scheme schedule (and corresponding stderr) 
    readonly pretty_day_scheme_schedule="$LOGS"/pretty_day_scheme_schedule_"$END_DATE".txt
    # Schemes that ran on desired day
    readonly desired_schemes="$LOGS"/desired_schemes_"$END_DATE".txt
    
    # 1. Determine which schemes ran on the desired day

    # pre_confinterval outputs each scheme's name as well as the days it ran
    # (Note this is dependent on the format of the output)
    if ! "$STATS_REPO_PATH"/pre_confinterval \
             "$desired_schemes" --build-schemedays-list \
             < ${STREAM_STATS_PREFIX}_"$END_DATE".txt 2> "$pretty_day_scheme_schedule"; then
        
        >&2 echo "Error determining day's schemes ($END_DATE); pre_confinterval exited unsuccessfully
                  or never started (see $pretty_day_scheme_schedule)"
        rm -f "$desired_schemes"
        exit 1
    fi
    
    # Convert to comma-separated list 
    set -o pipefail 
    local schemes # declare separately for pipefail 
    schemes=$(cat "$desired_schemes" | cut -d ' ' -f1 | tr '\n' ',')
    schemes=${schemes%?} # remove trailing comma 
    # Only needed to get desired schemes
    rm "$desired_schemes" 

    # Experiment duration is unknown, so can't anticipate how many stream stats
    # need to be downloaded (since the stream stats are needed to determine duration). 
    # So, download and compute intersection in 30-day chunks, stopping when a
    # chunk contains the beginning of the experiment.
    local duration_len=$CHUNK_LEN
    # Number of days known to be contiguous and available thus far, including END_DATE 
    # Initialize to 0 -- e.g. if entire first chunk is good, nchunk_good_days = chunk_len - 0
    local ncumulative_good_days=0
    # Number of contiguous, available days in the *most recent* chunk (not cumulative)
    local nchunk_good_days=$CHUNK_LEN
    # Continue as long as entire chunk is available and intersection is contiguous
    while (( nchunk_good_days == CHUNK_LEN )); do  
        # keep overwriting intersection until we reach the beginning of the experiment
        get_period_intersection $duration_len "$schemes" 
        unset contiguous_filenames
        readarray -t contiguous_filenames < <(list_contiguous_filenames) 
        local prev_ncumulative_good_days=$ncumulative_good_days 
        local ncumulative_good_days=${#contiguous_filenames[@]}
        let "nchunk_good_days = ncumulative_good_days - prev_ncumulative_good_days" || true
        let "duration_len += CHUNK_LEN" 
    done
}

scheme_stats() {
    unset contiguous_filenames
    readarray -t contiguous_filenames < <(list_contiguous_filenames)
    # Longest period to be plotted
    readonly expt_duration_len=${#contiguous_filenames[@]}
    # days per period
    declare -A time_periods 
    time_periods[day]=1 
    time_periods[week]=7 
    time_periods[two_weeks]=14 
    time_periods[month]=30 
    time_periods[duration]=$expt_duration_len

    # For each time_period: Calculate scheme stats and plot (or skip period)
    for time_period in ${!time_periods[@]}; do
        # Order of iteration over time_periods is unpredictable -- next period may be shorter
        local time_period_len="${time_periods["$time_period"]}"
        
        if (( $time_period_len > $expt_duration_len )); then
            # Skip period if duration is shorter
            echo "$end_date_prefix $time_period skipped"
            continue
        fi
        # Earliest day in period
        local period_start_inclusive=$(date -I -d "${END_DATE:14:10} - $time_period_len days")
        
        speeds=("all" "slow")  
        for speed in ${speeds[@]}; do
            local confint_out=${time_period}_${speed}_scheme_stats_"$END_DATE".txt
            local scheme_stats_err="$LOGS"/${time_period}_${speed}_scheme_stats_err_"$END_DATE".txt

            if ! cat "${contiguous_filenames[@]:0:$time_period_len}" |
                "$STATS_REPO_PATH"/confinterval \
                --scheme-intersection "$duration_scheme_intersection" \
                --stream-speed "$speed" \
                --watch-times "$LOCAL_DATA_PATH"/"$WATCH_TIMES_POSTFIX" \
                > "$confint_out" 2> "$scheme_stats_err"; then
                
                >&2 echo "Error generating scheme statistics ($END_DATE); confinterval exited unsuccessfully 
                          or never started (see $scheme_stats_err)" 
                rm -f "$confint_out"
                exit 1
            fi

            local plot=${time_period}_${speed}_plot_"$END_DATE".svg
            if [ $speed = "all" ]; then local plot_title_prefix="All stream speeds"
            else local plot_title_prefix="Slow streams only"; fi
            if [ $time_period_len -eq 1 ]; then local plot_title="$plot_title_prefix, $end_date_prefix"
            else local plot_title="$plot_title_prefix, $period_start_inclusive : $end_date_prefix"; fi

            if ! "$STATS_REPO_PATH"/plots/scheme_stats_to_plot.py \
                --title "$plot_title" --input_data "$confint_out" --output_figure "$plot" \
                2> "$scheme_stats_err"; then
                >&2 echo "Error plotting ($END_DATE); see $scheme_stats_err" 
                rm -f "$plot" 
                exit 1
            fi
        done
    done
}

main "$@"
