#!/bin/bash -ue

# For desired day: generate per-STREAM stats.
# For desired day/week/two weeks/month (if prereq files and data available locally):
    # Generate per-SCHEME stats and plots.  
    # (Note this introduces a dependency across days, so be careful parallelizing)

main() {
    # List of days all requested schemes were run
    readonly scheme_intersection="$LOGS"/scheme_intersection_"$END_DATE".txt
    
    # Avoid hang if logs directory not created yet/deleted
    mkdir -p "$LOCAL_DATA_PATH"/"$END_DATE"/"$LOGS" 
    cd "$LOCAL_DATA_PATH"/"$END_DATE" 
    
    # 1. Generate STREAM stats for desired day 
    stream_stats  
    
    # 2. Get filenames of stream stats over longest period (month)
    # 2a. Get dates
    unset month_dates
    MONTH_LEN=30
    readarray -t month_dates < <("$STATS_REPO_PATH"/scripts/list_period_dates.sh "$END_DATE" "$MONTH_LEN") 
    if [ "${#month_dates[@]}" -ne "$MONTH_LEN" ]; then
        >&2 echo "Error enumerating month dates before generating daily stream stats ($END_DATE)"
        exit 1
    fi

    # 2b. Map dates to filenames, for convenience
    declare -a month_stream_stats_files
    for i in ${!month_dates[@]}; do
        period_date="${month_dates[$i]}"
        stream_stats_file="$LOCAL_DATA_PATH"/"$period_date"/${STREAM_STATS_PREFIX}_"$period_date".txt
        month_stream_stats_files[$i]="$stream_stats_file"
    done
    
    # 3. Prerequisites for per-scheme analysis
    run_pre_confinterval
    
    # 4. Generate SCHEME stats/plots for desired day, week, two weeks, month 
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

run_pre_confinterval() {    
    # List of all days each scheme has run.
    # Note: All time periods can use the same scheme schedule
    # (time period only needs to be a subset of scheme schedule).
    # The stream stats input to confinterval contain exactly the desired time period,
    # so any days in scheme schedule outside the period are not considered when calculating scheme stats.
    readonly month_scheme_schedule="$LOGS"/month_scheme_schedule_"$END_DATE".txt
    # Readable summary of schemes run on desired day (and corresponding stderr) 
    readonly pretty_day_scheme_schedule="$LOGS"/pretty_day_scheme_schedule_"$END_DATE".txt
    # Readable summary of schemes run on desired month (and corresponding stderr)
    readonly pretty_month_scheme_schedule="$LOGS"/pretty_month_scheme_schedule_"$END_DATE".txt
    # Schemes that ran on desired day
    readonly desired_schemes="$LOGS"/desired_schemes_"$END_DATE".txt
    
    # 1. Determine which schemes ran on the desired day

    # pre_confinterval outputs each scheme's name as well as the days it ran
    # (Note this is dependent on the format of the output)
    if ! "$STATS_REPO_PATH"/pre_confinterval \
             "$desired_schemes" --build-schemedays-list \
             < ${STREAM_STATS_PREFIX}_"$END_DATE".txt 2> "$pretty_day_scheme_schedule"; then
        
        >&2 echo "Error determining day's schemes ($END_DATE); pre_confinterval exited unsuccessfully \
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

    # 2. Build scheme schedule using all stream stats available over 
    # the longest period to be analyzed (here, month)

    set +o pipefail # ignore errors from missing files
    if ! cat "${month_stream_stats_files[@]}" 2> /dev/null |
            "$STATS_REPO_PATH"/pre_confinterval \
            "$month_scheme_schedule" --build-schemedays-list \
            2> "$pretty_month_scheme_schedule"; then
             
        >&2 echo "Error building scheme schedule ($END_DATE); pre_confinterval exited unsuccessfully \
                  or never started (see $pretty_month_scheme_schedule)"
        rm -f "$month_scheme_schedule"
        exit 1
    fi

    # 3. Get intersection using scheme schedule
    # Note: confinterval ignores a day's data if the day is not in this list 
    # (meaning not all schemes ran that day)
    readonly intx_err="$LOGS"/intersection_log_"$END_DATE".txt
    if ! "$STATS_REPO_PATH"/pre_confinterval \
            "$month_scheme_schedule" --intersect-schemes "$schemes" \
            --intersect-out "$scheme_intersection" \
            2> "$intx_err"; then

        >&2 echo "Error generating scheme intersection ($END_DATE); pre_confinterval exited unsuccessfully \
                  or never started (see $intx_err)" 
        rm -f "$scheme_intersection" 
        exit 1
    fi
}

scheme_stats() {
    # days per period
    declare -A time_periods 
    time_periods[day]=1 
    time_periods[week]=7 
    time_periods[two_weeks]=14 
    time_periods[month]=30 

    # Read scheme intersection file into convenient format
    unset scheme_intx_ts
    readarray -t -d ' ' scheme_intx_ts < <(tail "$scheme_intersection" -n +2)
    unset scheme_intx_ts[${#scheme_intx_ts[@]}-1] # strip empty element
    declare -a scheme_intx_strings
    for i in ${!scheme_intx_ts[@]}; do
        # 11am ts => day string e.g. 2020-02-02
        local ts=${scheme_intx_ts[$i]}
        local day_str_prefix=$(date -I --date=@$ts --utc)
        local day_str_postfix=$(date -I -d "$day_str_prefix + 1 day")
        # Timestamps in file are sorted in increasing order, filenames we'll compare to are decreasing
        scheme_intx_strings[((${#scheme_intx_ts[@]}-$i-1))]=${day_str_prefix}T11_${day_str_postfix}T11 
    done 

    # For each time_period: Calculate scheme stats and plot (or skip period)
    for time_period in ${!time_periods[@]}; do
        # Order of iteration over time_periods is unpredictable -- next period may be shorter
        local ndays="${time_periods["$time_period"]}"
        
        # 1. Skip time period if not all stream stats available
        if ! ls "${month_stream_stats_files[@]:0:$ndays}" &> /dev/null; then
            echo "Skipping time period $time_period (ending $END_DATE) with insufficient stream stats available"
            continue
        fi
        
        # 2. Skip time period if not all schemes ran every day 
        local period_dates=${month_dates[@]:0:$ndays} 
        if [[ "${scheme_intx_strings[*]}" != *"$period_dates"* ]]; then
            echo "Skipping time period $time_period (ending $END_DATE) during which not all schemes ran daily"
            continue 
        fi 

        # Date of earliest day in period 
        local period_start_inclusive=${month_dates[(($ndays-1))]}
        
        speeds=("all" "slow")  
        for speed in ${speeds[@]}; do
            local confint_out=${time_period}_${speed}_scheme_stats_"$END_DATE".txt
            local scheme_stats_err="$LOGS"/${time_period}_${speed}_scheme_stats_err_"$END_DATE".txt

            if ! cat "${month_stream_stats_files[@]:0:$ndays}" |
                "$STATS_REPO_PATH"/confinterval \
                --scheme-intersection "$scheme_intersection" \
                --stream-speed "$speed" \
                --watch-times "$LOCAL_DATA_PATH"/"$WATCH_TIMES_POSTFIX" \
                > "$confint_out" 2> "$scheme_stats_err"; then
                
                >&2 echo "Error generating scheme statistics ($END_DATE); confinterval exited unsuccessfully \
                          or never started (see $scheme_stats_err)" 
                rm -f "$confint_out"
                exit 1
            fi

            local plot=${time_period}_${speed}_plot_"$END_DATE".svg
            if [ $ndays -eq 1 ]; then
                local plot_title="$speed speeds, $END_DATE (UTC)"
            else
                local plot_title="$speed speeds, $period_start_inclusive : $END_DATE (UTC)"
            fi

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
