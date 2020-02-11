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
#include <dateutil.hh>

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using namespace std::literals;

/** 
 * From stdin, parses output of analyze, which contains one line per stream summary.
 * To output file, writes a list of days each scheme has run, used to determine the dates to analyze.\n"
 */

/* Whenever a timestamp is used to represent a day, round down to Influx backup hour,
 * in seconds (analyze records ts as seconds) */
using Day_sec = uint64_t;

/* Program either writes a list of schemedays to file, or reads the list from file
 * and uses it to find the intersection of multiple schemes' days. */
enum Action {NONE, BUILD_LIST, INTERSECTION};

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

class SchemeDays {

    /* For each scheme, records all unique days the scheme ran, 
     * according to input data */
    map<string, set<Day_sec>> scheme_days{};

    /* File storing scheme_days */
    string scheme_days_filename;

    public: 
    // Populate scheme_days map
    SchemeDays (const string & scheme_days_filename, Action action): 
                scheme_days_filename(scheme_days_filename) {  
        if (action == BUILD_LIST) {
            // populate from stdin (i.e. analyze output)
            parse_stdin(); 
        } else {
            // populate from input file 
            read_scheme_days();
        }
    }

    /* Populate scheme_days map from stdin */
    void parse_stdin() {
        ios::sync_with_stdio(false);
        string line_storage;

        unsigned int line_no = 0;

        vector<string_view> fields;
        vector<string_view> scratch;

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

            if (line.size() > 500) {
                throw runtime_error("Line " + to_string(line_no) + " too long");
            }

            split_on_char(line, ' ', fields);
            if (fields.size() != 18) {
                throw runtime_error("Bad line: " + line_storage);
            }

            string_view& ts_str = fields[0];
            string_view& scheme = fields[4];

            const uint64_t ts = to_uint64(ts_str);

            /* Record this stream's day for the corresponding scheme, 
             * regardless of stream characteristics */
            record_scheme_day(ts, string(scheme));
        }   
    }

    /* Given the base timestamp and scheme of a stream, add 
     * corresponding day to the set of days the scheme was run.
     * Does not assume input data is sorted in any way. */ 
    void record_scheme_day(uint64_t ts, const string & scheme) {
        Day_sec day = ts2Day_sec(ts);
        scheme_days[scheme].emplace(day);
    }

    /* Read scheme days from filename into scheme_days map */
    void read_scheme_days() {
        ifstream scheme_days_file {scheme_days_filename};
        if (not scheme_days_file.is_open()) {
            throw runtime_error( "can't open " + scheme_days_filename );
        }

        string line_storage;
        string scheme;
        Day_sec day;

        while (getline(scheme_days_file, line_storage)) {
            istringstream line(line_storage);
            line >> scheme;
            while (line >> day) {   // read all days
                scheme_days[scheme].emplace(day);
            }
        } 
        scheme_days_file.close();   
        if (scheme_days_file.bad()) {
            throw runtime_error("error reading " + scheme_days_filename);
        }
    }


    /* Write map of scheme days to file */
    void write_scheme_days() {
        ofstream scheme_days_file;
        scheme_days_file.open(scheme_days_filename);
        if (not scheme_days_file.is_open()) {
            throw runtime_error( "can't open " + scheme_days_filename);
        }
        // line format:
        // mpc/bbr 1565193009 1567206883 1567206884 1567206885 ...
        for (const auto & [scheme, days] : scheme_days) {
            scheme_days_file << scheme;
            for (const Day_sec & day : days) {
                scheme_days_file << " " << day;
            }
            scheme_days_file << "\n";
        }
        scheme_days_file.close();   
        if (scheme_days_file.bad()) {
            throw runtime_error("error writing " + scheme_days_filename);
        }
    }
    
    void print_summary() {
        cerr << "In-memory scheme_days:\n";
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

void scheme_days_main(const string & scheme_days_filename, const string & desired_schemes,
                      const string & intersection_filename, Action action) {
    // populates map from input data or file
    SchemeDays scheme_days {scheme_days_filename, action};
    if (action == BUILD_LIST) {
        /* Analyze output => scheme days file */
        scheme_days.write_scheme_days(); 
        scheme_days.print_summary();    
    } else {
        /* Desired schemes, scheme days file => intersecting days */
        scheme_days.intersect(desired_schemes, intersection_filename);    
    }
}

void print_usage(const string & program) {
    cerr << "Usage: " << program << " <scheme_days_filename> <action>\n" 
        << "Action: One of\n" 
        << "\t --build-list: Read analyze output from stdin, and write to scheme_days_filename "
        "the list of days each scheme was run \n"
        << "\t --intersect-schemes <schemes> --intersect-outfile <intersection_filename>: For the given schemes "
        "(i.e. primary, vintages, or comma-separated list e.g. mpc/bbr,puffer_ttp_cl/bbr), "
        "read from scheme_days_filename, and write to intersection_filename the schemes and intersecting days\n";
}

int main(int argc, char *argv[]) {
    try {
        if (argc < 1) {
            abort();
        }

        const option actions[] = {
            {"build-list", no_argument, nullptr, 'b'},
            {"intersect-schemes", required_argument, nullptr, 's'},
            {"intersect-outfile", required_argument, nullptr, 'o'},
            {nullptr, 0, nullptr, 0}
        };
        Action action = NONE;
        string desired_schemes; 
        string intersection_filename;

        while (true) {
            const int opt = getopt_long(argc, argv, "bo:s:", actions, nullptr);
            if (opt == -1) break;
            switch (opt) {
                case 'b':
                    if (action == INTERSECTION) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = BUILD_LIST;
                    break;
                case 's':
                    if (action == BUILD_LIST) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = INTERSECTION;
                    desired_schemes = optarg;
                    break;
                case 'o':
                    if (action == BUILD_LIST) {
                        cerr << "Error: Only one action can be selected\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    action = INTERSECTION;
                    intersection_filename = optarg;
                    break;
                default:
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
            }
        }

        if (optind != argc - 1 or action == NONE) {
            cerr << "Error: Filename and action are required\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if (action == INTERSECTION and (desired_schemes.empty() or intersection_filename.empty())) {
            cerr << "Error: Intersection requires schemes list and outfile\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        string scheme_days_filename = argv[optind]; 
        scheme_days_main(scheme_days_filename, desired_schemes, intersection_filename, action);

    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
