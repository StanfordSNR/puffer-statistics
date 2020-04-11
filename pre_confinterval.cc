#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include <vector>
#include <string>
#include <array>
#include <tuple>
#include <charconv>
#include <map>
#include <cstring>
#include <fstream>
#include <random>
#include <algorithm>
#include <iomanip>
#include <getopt.h>
#include <cassert>
#include <set>
#include "dateutil.hh"
#include "confintutil.hh"

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using namespace std::literals;

/** 
 * From stdin, parses output of analyze, which contains one line per stream summary.
 * Takes *one* of the following actions:
 * 1. Write a list of days each scheme has run (used to find intersection)
 * 2. Find the intersection of multiple schemes' days (used to determine the dates to analyze)
 * 3. Write two lists of watchtimes (used to sample random watch times), 
 *    one for slow streams and one for all. */
enum Action {NONE, SCHEMEDAYS_LIST, INTERSECT, WATCHTIMES_LIST};

/* Whenever a timestamp is used to represent a day, round down to Influx backup hour,
 * in seconds (analyze records ts as seconds) */
using Day_sec = uint64_t;

size_t memcheck() {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) < 0) {
        perror("getrusage");
        throw runtime_error(string("getrusage: ") + strerror(errno));
    }

    if (usage.ru_maxrss > 12 * 1024 * 1024) {
        throw runtime_error("memory usage is at " + to_string(usage.ru_maxrss) + " KiB");
    }

    return usage.ru_maxrss;
}

void split_on_char(const string_view str, const char ch_to_find, vector<string_view> & ret) {
    ret.clear();

    bool in_double_quoted_string = false;
    unsigned int field_start = 0;
    for (unsigned int i = 0; i < str.size(); i++) {
        const char ch = str[i];
        if (ch == '"') {
            in_double_quoted_string = !in_double_quoted_string;
        } else if (in_double_quoted_string) {
            continue;
        } else if (ch == ch_to_find) {
            ret.emplace_back(str.substr(field_start, i - field_start));
            field_start = i + 1;
        }
    }

    ret.emplace_back(str.substr(field_start));
}

uint64_t to_uint64(string_view str) {
    uint64_t ret = -1;
    const auto [ptr, ignore] = from_chars(str.data(), str.data() + str.size(), ret);
    if (ptr != str.data() + str.size()) {
        str.remove_prefix(ptr - str.data());
        throw runtime_error("could not parse as integer: " + string(str));
    }

    return ret;
}

double to_double(const string_view str) {
    /* sadly, g++ 8 doesn't seem to have floating-point C++17 from_chars() yet
       float ret;
       const auto [ptr, ignore] = from_chars(str.data(), str.data() + str.size(), ret);
       if (ptr != str.data() + str.size()) {
       throw runtime_error("could not parse as float: " + string(str));
       }

       return ret;
       */

    /* apologies for this */
    char * const null_byte = const_cast<char *>(str.data() + str.size());
    char old_value = *null_byte;
    *null_byte = 0;

    const double ret = atof(str.data());

    *null_byte = old_value;

    return ret;
}

class SchemeDays {

    /* For each scheme, records all unique days the scheme ran, 
     * according to input data */
    map<string, set<Day_sec>> scheme_days{};

    /* Records all watch times, in order of input data. */
    vector<double> all_watch_times{}; 
    /* Records slow-stream watch times, in order of input data. */
    vector<double> slow_watch_times{}; 

    /* File storing scheme_days or watch_times */
    string list_filename;

    public: 
    // Populate scheme_days or watch_times map
    SchemeDays (const string & list_filename, Action action): 
                list_filename(list_filename) {  
        if (action == SCHEMEDAYS_LIST or action == WATCHTIMES_LIST) {
            // populate from stdin (i.e. analyze output)
            parse_stdin(action); 
        } else if (action == INTERSECT) {
            // populate from input file 
            read_scheme_days();
        }
    }

    /* Populate scheme_days or watch_times map from stdin */
    void parse_stdin(Action action) {
        ios::sync_with_stdio(false);
        string line_storage;

        unsigned int line_no = 0;

        vector<string_view> fields;

        while (cin.good()) {
            if (line_no % 1000000 == 0) {
                const size_t rss = memcheck() / 1024;
                cerr << "line " << line_no / 1000000 << "M, RSS=" << rss << " MiB\n";
            }

            getline(cin, line_storage);
            line_no++;

            const string_view line{line_storage};

            // ignore lines marked with # (by analyze)
            if (line.empty() or line.front() == '#') {
                continue;
            }

            if (line.size() > MAX_LINE_LEN) {
                throw runtime_error("Line " + to_string(line_no) + " too long");
            }

            split_on_char(line, ' ', fields);
            if (fields.size() != N_STREAM_STATS) {  
                throw runtime_error("Line has " + to_string(fields.size()) + " fields, expected "
                                    + to_string(N_STREAM_STATS) + ": " + line_storage);
            }

            const auto & [timestamp, goodbad, fulltrunc, badreason, scheme, 
                  extent, usedpct, mean_ssim, mean_delivery_rate, average_bitrate, ssim_variation_db,
                  startup_delay, time_after_startup,
                  time_stalled]
                      = tie(fields[0], fields[1], fields[2], fields[3],
                              fields[4], fields[5], fields[6], fields[7],
                              fields[8], fields[9], fields[10], fields[11],
                              fields[12], fields[13]);

            if (action == SCHEMEDAYS_LIST) {
                /* Record this stream's day for the corresponding scheme, 
                 * regardless of stream characteristics */
                record_scheme_day(timestamp, scheme);
            } else if (action == WATCHTIMES_LIST) {
                /* Record this stream's watch time, 
                 * regardless of stream characteristics except delivery rate */
                record_watch_time(mean_delivery_rate, time_after_startup);
            }
        }   
    }

    void record_watch_time(const string_view & mean_delivery_rate, 
                           const string_view & time_after_startup) {
        vector<string_view> scratch;
        split_on_char(mean_delivery_rate, '=', scratch);
        if (scratch[0] != "mean_delivery_rate"sv) {
            throw runtime_error("delivery rate field mismatch");
        }
        const double delivery_rate = to_double(scratch[1]);

        split_on_char(time_after_startup, '=', scratch);
        if (scratch[0] != "total_after_startup"sv) {
            throw runtime_error("watch time field mismatch");
        }

        const double watch_time = to_double(scratch[1]);
        if (watch_time < (1 << MIN_BIN) or watch_time > (1 << MAX_BIN)) {
            return;   // TODO: check this is what we want. Also, should we ignore wt > max in confint? rn, would throw
        }

        all_watch_times.push_back(watch_time);
        if (stream_is_slow(delivery_rate)) {
            slow_watch_times.push_back(watch_time);
        }
    }

    /* Given the base timestamp and scheme of a stream, add 
     * corresponding day to the set of days the scheme was run.
     * Does not assume input data is sorted in any way. */ 
    void record_scheme_day(const string_view & timestamp, 
                           const string_view & scheme) {
        vector<string_view> scratch;
        split_on_char(timestamp, '=', scratch);
        if (scratch[0] != "ts"sv) {
            throw runtime_error("timestamp field mismatch");
        }

        const uint64_t ts = to_uint64(scratch[1]);

        split_on_char(scheme, '=', scratch);
        if (scratch[0] != "scheme"sv) {
            throw runtime_error("scheme field mismatch");
        }

        string_view schemesv = scratch[1];
        Day_sec day = ts2Day_sec(ts);
        scheme_days[string(schemesv)].emplace(day);
    }

    /* Read scheme days from filename into scheme_days map */
    void read_scheme_days() {
        ifstream list_file {list_filename};
        if (not list_file.is_open()) {
            throw runtime_error( "can't open " + list_filename );
        }

        string line_storage;
        string scheme;
        Day_sec day;

        while (getline(list_file, line_storage)) {
            istringstream line(line_storage);
            if (not (line >> scheme)) {
                throw runtime_error("error reading scheme from " + list_filename);
            }
            while (line >> day) {   // read all days
                scheme_days[scheme].emplace(day);
            }
        } 
        list_file.close();   
        if (list_file.bad()) {
            throw runtime_error("error reading " + list_filename);
        }
    }

    /* Write map of scheme days to file */
    void write_scheme_days() {
        ofstream list_file {list_filename};
        if (not list_file.is_open()) {
            throw runtime_error( "can't open " + list_filename);
        }
        // line format:
        // mpc/bbr 1565193009 1567206883 1567206884 1567206885 ...
        for (const auto & [scheme, days] : scheme_days) {
            list_file << scheme;
            for (const Day_sec & day : days) {
                list_file << " " << day;
            }
            list_file << "\n";
        }
        list_file.close();   
        if (list_file.bad()) {
            throw runtime_error("error writing " + list_filename);
        }
    }
        
    /* Write map of watch times to slow and all files */
    void write_watch_times() {
        const string & all_full_filename = "all_" + list_filename;
        const string & slow_full_filename = "slow_" + list_filename;
        ofstream all_list_file{all_full_filename};
        if (not all_list_file.is_open()) {
            throw runtime_error( "can't open " + all_full_filename);
        }
        ofstream slow_list_file{slow_full_filename};
        if (not slow_list_file.is_open()) {
            throw runtime_error( "can't open " + slow_full_filename);
        }

        // file is single line of space-separated times
        for (const double watch_time : all_watch_times) {
            all_list_file << watch_time << " ";
        }
        for (const double slow_watch_time : slow_watch_times) {
            slow_list_file << slow_watch_time << " ";
        }

        all_list_file.close();   
        if (all_list_file.bad()) {
            throw runtime_error("error writing " + all_full_filename);
        }
        slow_list_file.close();   
        if (slow_list_file.bad()) {
            throw runtime_error("error writing " + slow_full_filename);
        }
    }

    /* Human-readable summary of days each scheme ran 
     * (output file is not particularly fun to read). */
    void print_schemedays_summary() {
        cerr << "Scheme schedule:\n";
        for (const auto & [scheme, days] : scheme_days) {
            cerr << "\n" << scheme << "\n"; 
            print_intervals(days);
        }
    }

    /* Intersection of all days the requested
     * schemes were run, according to scheme_days */
    void intersect(const string & desired_schemes_unparsed,
                   const string & intersection_filename) {
        // parse list of schemes
        vector<string> desired_schemes{};

        if (desired_schemes_unparsed == "primary") {
            desired_schemes = 
                {"puffer_ttp_cl/bbr", "mpc/bbr", 
                 "robust_mpc/bbr", "pensieve/bbr", "linear_bba/bbr"}; 
        } else if (desired_schemes_unparsed == "vintages") {
            desired_schemes = 
            {"puffer_ttp_cl/bbr", "puffer_ttp_20190202/bbr", 
             "puffer_ttp_20190302/bbr", "puffer_ttp_20190402/bbr", "puffer_ttp_20190502/bbr"};
        } else {
            // comma-sep list
            vector<string_view> desired_schemes_sv{};
            split_on_char(desired_schemes_unparsed, ',', desired_schemes_sv);
            for (const auto & desired_scheme : desired_schemes_sv) {
                desired_schemes.emplace_back(desired_scheme);
            }
        }
        // find intersection
        set<Day_sec> running_intx {scheme_days[desired_schemes.front()]};
        set<Day_sec> cur_intx {};
        for (auto it = desired_schemes.begin() + 1; it != desired_schemes.end(); it++) {
            if (scheme_days[*it].empty()) {
                throw runtime_error("requested scheme " + *it + " was not run on any days");
            }
            set_intersection(running_intx.cbegin(), running_intx.cend(), 
                             scheme_days[*it].cbegin(), scheme_days[*it].cend(),
                             inserter(cur_intx, cur_intx.begin()));
            swap(running_intx, cur_intx);
            cur_intx.clear();
        }
        if (running_intx.empty()) {
            throw runtime_error("requested schemes were not run on any intersecting days");
        }
        
        /* Write intersection to file (along with schemes, so 
         * confinterval doesn't need to take schemes as arg) */
        ofstream intersection_file;
        intersection_file.open(intersection_filename);
        if (not intersection_file.is_open()) {
            throw runtime_error( "can't open " + intersection_filename);
        }
        for (const auto & scheme : desired_schemes) {
            intersection_file << scheme << " ";
        }
        intersection_file << "\n";
        // file format:
        // robust_mpc/bbr mpc/bbr ...
        // 1565193009 1567206883 1567206884 1567206885 ...
        for (const auto & day : running_intx) {
            intersection_file << day << " ";
        }
        intersection_file << "\n";     
        intersection_file.close();
        if (intersection_file.bad()) {
            throw runtime_error("error writing " + intersection_filename);
        }
    }
};

void scheme_days_main(const string & list_filename, const string & desired_schemes,
                      const string & intersection_filename, Action action) {
    // Populates schemedays/watchtimes map from input data or file
    SchemeDays scheme_days {list_filename, action};
    if (action == SCHEMEDAYS_LIST) {
        /* Scheme days map => scheme days file */
        scheme_days.write_scheme_days(); 
        scheme_days.print_schemedays_summary();    
    } else if (action == INTERSECT) {
        /* Desired schemes, scheme days file => intersecting days */
        scheme_days.intersect(desired_schemes, intersection_filename);    
    } else if (action == WATCHTIMES_LIST) {
        /* Watch times map => watch times file */
        scheme_days.write_watch_times(); 
    }
}

void print_usage(const string & program) {
    cerr << "Usage: " << program << " <list_filename> <action>\n" 
         << "Action: One of\n" 
         << "\t --build-schemedays-list: Read analyze output from stdin, and write to list_filename "
            "the list of days each scheme was run \n"
         << "\t --intersect-schemes <schemes> --intersect-outfile <intersection_filename>: For the given schemes "
            "(i.e. primary, vintages, or comma-separated list e.g. mpc/bbr,puffer_ttp_cl/bbr), "
            "read from list_filename, and write to intersection_filename the schemes and intersecting days\n"
         << "\t --build-watchtimes-list: Read analyze output from stdin, and write the watch times to "
            "slow_list_filename and all_list_filename (separate file for slow streams)\n";
}

int main(int argc, char *argv[]) {
    try {
        if (argc < 1) {
            abort();
        }

        const option actions[] = {
            {"build-schemedays-list", no_argument, nullptr, 'd'},
            {"intersect-schemes", required_argument, nullptr, 's'},
            {"intersect-outfile", required_argument, nullptr, 'o'},
            {"build-watchtimes-list", no_argument, nullptr, 'w'},
            {nullptr, 0, nullptr, 0}
        };
        Action action = NONE;
        string desired_schemes; 
        string intersection_filename;

        while (true) {
            const int opt = getopt_long(argc, argv, "ds:o:w", actions, nullptr);
            if (opt == -1) break;
            switch (opt) {
                case 'd':
                    if (action != NONE) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = SCHEMEDAYS_LIST;
                    break;
                case 's':
                    if (action != NONE and action != INTERSECT) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = INTERSECT;
                    desired_schemes = optarg;
                    break;
                case 'o':
                    if (action != NONE and action != INTERSECT) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = INTERSECT;
                    intersection_filename = optarg;
                    break;
                case 'w':
                    if (action != NONE) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = WATCHTIMES_LIST;
                    break;
                default:
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
            }
        }

        if (optind != argc - 1 or action == NONE) {
            cerr << "Error: List_filename and action are required\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if (action == INTERSECT and (desired_schemes.empty() or intersection_filename.empty())) {
            cerr << "Error: Intersection requires schemes list and outfile\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        string list_filename = argv[optind];     
        scheme_days_main(list_filename, desired_schemes, intersection_filename, action);

    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
