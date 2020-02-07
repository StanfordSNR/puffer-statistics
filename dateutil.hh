/* Date utilities, useful for analyze/confinterval/schemedays. */

#ifndef DATEUTIL_HH
#define DATEUTIL_HH

#include <set>
#include <iostream>
#include <string>

using std::cerr;    using std::string; 
using Day_sec = uint64_t;

/* Hour of Influx backup, e.g. 11 for 11am UTC, 23 for 11pm UTC.
 * TODO: can we pull this number from somewhere? */
static const unsigned BACKUP_HR = 11;

/** 
 * Given a set of Unix timestamps, prints contiguous intervals as nice strings. 
 * E.g. given {Jan 15, Jan 16, Jan 17, Jan 18, Feb 1, Feb 2, Feb 3},
 * print Jan 15 : Jan 18, Feb 1 : Feb 3.
 * Useful for schemedays and confinterval.
 */
void print_intervals(const std::set<uint64_t> & days) {
    struct tm ts_fields{};
    char day_str[80];
    char prev_day_str[80];
    uint64_t prev_day = -1;

    for (const auto & day: days) {  // set is ordered
        // swap at beginning of loop iter so day_str is the current day at loop exit
        std::swap(prev_day_str, day_str);    
        time_t ts_raw = day;
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

/* Round down ts *in seconds* to nearest backup hour. */
Day_sec ts2Day_sec(uint64_t ts) {
    if (ts > 9999999999) {
        throw(std::logic_error("ts2Day_sec operates on seconds, not nanoseconds"));
    }
    const unsigned sec_per_day = 60 * 60 * 24;
    return ts / sec_per_day * sec_per_day + BACKUP_HR * 60 * 60;
}

#endif
