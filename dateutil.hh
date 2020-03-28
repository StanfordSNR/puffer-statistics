/* Date utilities, useful for analyze/(pre)confinterval. */

#ifndef DATEUTIL_HH
#define DATEUTIL_HH

#include <set>
#include <iostream>
#include <string>

using std::cerr;    using std::string;  

using Day_sec = uint64_t;

/* Hour of Influx backup, e.g. 11 for 11am UTC, 23 for 11pm UTC.
 * TODO: can we pull this number from somewhere? "T11" maybe? */
static const unsigned BACKUP_HR = 11;

/** 
 * Given a set of Unix timestamps (seconds), prints contiguous intervals as nice strings. 
 * E.g. given timestamps representing {Jan 15, Jan 16, Jan 17, Jan 18, Feb 1, Feb 2, Feb 3},
 * print Jan 15 : Jan 18, Feb 1 : Feb 3.
 * Useful for schemedays and confinterval.
 */
void print_intervals(const std::set<Day_sec> & days) {
    struct tm ts_fields{};
    char day_str[80];
    char prev_day_str[80];
    uint64_t prev_day = -1;

    for (const auto & day: days) {  // set is ordered
        // swap at beginning of loop iter so day_str is the current day at loop exit
        std::swap(prev_day_str, day_str);    
        time_t ts_raw = day;
        // gmtime assumes argument is GMT
        ts_fields = *gmtime(&ts_raw);
        strftime(day_str, sizeof(day_str), "%Y-%m-%d", &ts_fields);
        if (difftime(day, prev_day) > 60 * 60 * 24) {
            if (prev_day != -1UL) { 
                cerr << prev_day_str << "\n"; 
            }
            cerr << day_str << " : ";
        }
        prev_day = day;
    }
    // print end date
    if (not days.empty()) {
        cerr << day_str << "\n"; 
    }
}

/* Parse date string to Unix timestamp (seconds) at Influx backup hour, 
 * e.g. 2019-11-28T11_2019-11-29T11 => 1574938800 (for 11AM UTC backup)
 * Note expected format of date string. */
std::optional<Day_sec> str2Day_sec(const string & date_str) {
    // TODO: check format
    const auto T_pos = date_str.find('T');
    const string & start_day = date_str.substr(0, T_pos);

    struct tm day_fields{};
    std::ostringstream strptime_str;
    strptime_str << start_day << " " << BACKUP_HR << ":00:00";
    if (not strptime(strptime_str.str().c_str(), "%Y-%m-%d %H:%M:%S", &day_fields)) {
        return {};
    }

    // set timezone to UTC for mktime
    char* tz = getenv("TZ");
    setenv("TZ", "UTC", 1);
    tzset();

    Day_sec start_ts = mktime(&day_fields);

    tz ? setenv("TZ", tz, 1) : unsetenv("TZ");
    tzset();
    return start_ts;
}

/* Round down ts *in seconds* to nearest backup hour. */
Day_sec ts2Day_sec(uint64_t ts) {
    if (ts > 9999999999) {
        throw(std::logic_error("ts2Day_sec operates on seconds, not nanoseconds"));
    }
    const unsigned sec_per_hr = 60 * 60;
    const unsigned sec_per_day = sec_per_hr * 24;
    unsigned day_index = ts / sec_per_day;
    const unsigned sec_past_midnight = ts % sec_per_day;
    if (sec_past_midnight < BACKUP_HR * sec_per_hr) {
        /* If ts is before backup hour, it belongs to the previous day's backup 
         * (e.g. Jan 2 1:00 am belongs to Jan 1 11:00 am backup) */
        day_index--;
    }
   
    return day_index * sec_per_day + BACKUP_HR * sec_per_hr;
}

#endif

