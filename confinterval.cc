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
#include <dateutil.hh>

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using namespace std::literals;

/** 
 * From stdin, parses output of analyze, which contains one line per stream summary.
 * To stdout, outputs each scheme's mean stall ratio, SSIM, and SSIM variance,
 * along with confidence intervals. 
 * Stall ratio is calculated over simulated samples;
 * SSIM/SSIMvar is calculated over real samples.
 */

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

double raw_ssim_to_db(const double raw_ssim) {
    return -10.0 * log10( 1 - raw_ssim );
}

struct SchemeStats {
    // min and max acceptable watch time bin indices (inclusive)
    constexpr static unsigned int MIN_BIN = 2;
    constexpr static unsigned int MAX_BIN = 20;
    // Stall ratio data from *real* distribution
    array<vector<double>, 32> binned_stall_ratios{};

    unsigned int samples = 0;
    double total_watch_time = 0;
    double total_stall_time = 0;    

    // SSIM data from *real* distribution
    vector<pair<double,double>> ssim_samples{};
    vector<double> ssim_variation_samples{};

    double total_ssim_watch_time = 0;

    // bin = log(watch time), if watch time : [2^2, 2^20] 
    static unsigned int watch_time_bin(const double raw_watch_time) {
        const unsigned int watch_time_bin = lrintf(floorf(log2(raw_watch_time)));    
        if (watch_time_bin < MIN_BIN or watch_time_bin > MAX_BIN) {
            throw runtime_error("binned watch time error");
        }
        return watch_time_bin;
    }

    // add stall ratio to appropriate bin
    void add_sample(const double watch_time, const double stall_time) {
        binned_stall_ratios.at(watch_time_bin(watch_time)).push_back(stall_time / watch_time);

        samples++;
        total_watch_time += watch_time;
        total_stall_time += stall_time;
    }

    void add_ssim_sample(const double watch_time, const double mean_ssim) {
        if (mean_ssim <= 0 or mean_ssim > 1) {
            throw runtime_error("invalid ssim: " + to_string(mean_ssim));
        }
        total_ssim_watch_time += watch_time;
        ssim_samples.emplace_back(watch_time, mean_ssim);
    }

    void add_ssim_variation_sample(const double ssim_variation) {
        if (ssim_variation <= 0 or ssim_variation >= 1000) {
            throw runtime_error("invalid ssim variation: " + to_string(ssim_variation));
        }
        ssim_variation_samples.push_back(ssim_variation);
    }


    double observed_stall_ratio() const {
        return total_stall_time / total_watch_time;
    }

    double mean_ssim() const {
        double sum = 0;
        for ( const auto [watch_time, ssim] : ssim_samples ) {
            sum += watch_time * ssim;
        }
        return sum / total_ssim_watch_time;
    }

    double stddev_ssim() const {
        const double mean = mean_ssim();
        double ssr = 0;
        for ( const auto [watch_time, ssim] : ssim_samples ) {
            ssr += watch_time * (ssim - mean) * (ssim - mean);
        }
        const double variance = ssr / total_ssim_watch_time;
        return sqrt(variance);
    }

    tuple<double, double, double> sem_ssim() const {
        double sum_squared_weights = 0;
        for ( const auto [watch_time, ssim] : ssim_samples ) {
            sum_squared_weights += (watch_time * watch_time) / (total_ssim_watch_time * total_ssim_watch_time);
        }
        const double mean = mean_ssim();
        const double stddev = stddev_ssim();
        const double sem = stddev * sqrt(sum_squared_weights);
        return { raw_ssim_to_db( mean - 2 * sem ), raw_ssim_to_db( mean ), raw_ssim_to_db( mean + 2 * sem ) };
    }

    double mean_ssim_variation() const {
        return accumulate(ssim_variation_samples.begin(), ssim_variation_samples.end(), 0.0) / ssim_variation_samples.size();
    }

    double stddev_ssim_variation() const {
        const double mean = mean_ssim_variation();
        double ssr = 0;

        cerr << "count: " << ssim_variation_samples.size() << ", mean=" << mean << "\n";

        for ( const auto x : ssim_variation_samples ) {
            ssr += (x - mean) * (x - mean);
        }
        const double variance = (1.0 / (ssim_variation_samples.size() - 1)) * ssr;
        return sqrt(variance);
    }

    tuple<double, double, double> sem_ssim_variation() const {
        const double mean = mean_ssim_variation();
        const double sem = stddev_ssim_variation() / sqrt(ssim_variation_samples.size());
        return { mean - 2 * sem, mean, mean + 2 * sem };
    }
};

class Statistics {
    // list of watch times from which to sample
    vector<double> all_watch_times{};

    using Day = uint64_t;

    /* Days to be analyzed (read from input file) */
    set<Day> acceptable_days{};

    // real (non-simulated) stats 
    map<string, SchemeStats> scheme_stats{};

    public:     // TODO: check private/public (here and schemedays)
     Statistics (const string & intersection_filename) {
        vector<string> desired_schemes;
        /* Read file containing desired schemes, and list of days they intersect */
        read_intersection_file(intersection_filename, desired_schemes);
        // Initialize scheme_stats, so parse() knows the desired schemes
        for (const string & scheme : desired_schemes) {
            scheme_stats[scheme] = SchemeStats{};
        }
    }
        
     void read_intersection_file(const string & intersection_filename, vector<string> & desired_schemes) {
        ifstream intersection_file;
        intersection_file.open(intersection_filename);
        if (not intersection_file.is_open()) {
            throw runtime_error( "can't open " + intersection_filename);
        }
        string line_storage, scheme;
        // read all schemes
        if (!getline(intersection_file, line_storage)) {
            throw runtime_error( "error reading schemes from " + intersection_filename);
        }
        istringstream schemes_line(line_storage);
        while (schemes_line >> scheme) {    
            desired_schemes.emplace_back(scheme);
        }
        // read all days
        if (!getline(intersection_file, line_storage)) {
            throw runtime_error( "error reading dates from " + intersection_filename);
        } 
        Day day;
        istringstream days_line(line_storage);
        while (days_line >> day) {  
            acceptable_days.emplace(day);
        }
        intersection_file.close();
        if (intersection_file.bad()) {
            throw runtime_error("error reading " + intersection_filename);
        }
        cerr << "acceptable days: \n";  // TODO: remove
        print_intervals(acceptable_days);
     }

    /* Indicates whether ts is one of the acceptable days read
     * from the input file */
    bool ts_is_acceptable(uint64_t ts) {
        const unsigned sec_per_day = 60 * 60 * 24;
        // acceptable times are rounded down to nearest day
        Day day = ts / sec_per_day * sec_per_day;
        return (acceptable_days.count(day));
    }
    
    /* Populate SchemeStats with per-scheme watch/stall/ssim, 
     * ignoring stream if stream is bad/outside study period/short watch time.
     * Record all watch times independent of scheme. 
     * Record days each scheme was run, if desired. */
    void parse_stdin(bool slow_sessions) {
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
            if (fields.size() != 20) {
                throw runtime_error("Bad line: " + line_storage);
            }

            const auto & [ts_str, goodbad, fulltrunc, badreason, scheme, ip, os, channelchange, init_id,
                  extent, usedpct, mean_ssim, mean_delivery_rate, average_bitrate, ssim_variation_db,
                  startup_delay, time_after_startup,
                  time_stalled, total_stream_chunks, high_ssim_stream_chunks]
                      = tie(fields[0], fields[1], fields[2], fields[3],
                              fields[4], fields[5], fields[6], fields[7],
                              fields[8], fields[9], fields[10], fields[11],
                              fields[12], fields[13], fields[14], fields[15], fields[16], fields[17],
                              fields[18], fields[19]);

            const uint64_t ts = to_uint64(ts_str);

            if (not ts_is_acceptable(ts)) {
                cerr << "skipping out-of-range ts: ";
                print_intervals(set<uint64_t>{ts});  // TODO: remove
                continue;
            } else {
                cerr << "in-range ts: ";
                print_intervals(set<uint64_t>{ts});  // TODO: remove
            }

            if (slow_sessions) {
                split_on_char(mean_delivery_rate, '=', scratch);
                if (scratch[0] != "mean_delivery_rate"sv) {
                    throw runtime_error("field mismatch");
                }
                const double delivery_rate = to_double(scratch[1]);
                if (delivery_rate > (6000000.0/8.0)) {
                    continue;
                }
            }
            split_on_char(time_after_startup, '=', scratch);
            if (scratch[0] != "total_after_startup"sv) {
                throw runtime_error("field mismatch");
            }

            const double watch_time = to_double(scratch[1]);

            if (watch_time < 4) {
                continue;
            }

            // record distribution of *all* watch times (independent of scheme)
            all_watch_times.push_back(watch_time);

            split_on_char(time_stalled, '=', scratch);
            if (scratch[0] != "stall_after_startup"sv) {
                throw runtime_error("stall field mismatch");
            }

            const double stall_time = to_double(scratch[1]);

            // record ssim if available
            split_on_char(mean_ssim, '=', scratch);
            if (scratch[0] != "mean_ssim"sv) {
                throw runtime_error("ssim field mismatch");
            }

            const double mean_ssim_val = to_double(scratch[1]);

            // record ssim variation if available
            split_on_char(ssim_variation_db, '=', scratch);
            if (scratch[0] != "ssim_variation_db"sv) {
                throw runtime_error("ssimvar field mismatch");
            }

            const double ssim_variation_db_val = to_double(scratch[1]);
            
            // EXCLUDE BAD (but not trunc)
            if (goodbad == "bad") {  
                continue;
            }

            // Record stall ratio, ssim, ssim variation 
            // Ignore if not one of the requested schemes 
            SchemeStats *the_scheme = nullptr;
            auto found_scheme = scheme_stats.find(string(scheme));
            if (found_scheme != scheme_stats.end()) {
                the_scheme = &found_scheme->second;
            }

            if (the_scheme) {
                the_scheme->add_sample(watch_time, stall_time);
                if ( mean_ssim_val >= 0 ) { the_scheme->add_ssim_sample(watch_time, mean_ssim_val); }
                // SSIM variation = 0 over a whole stream is questionable
                if ( ssim_variation_db_val > 0 and ssim_variation_db_val <= 10000 ) { the_scheme->add_ssim_variation_sample(ssim_variation_db_val); }
            }
        }   // end while
    }

    /* Simulate watch and stall time: 
     * Draw a random watch time from all watch times; 
     * draw a stall ratio from the bin corresponding to the simulated watch time, 
     * in the per-scheme stall ratio distribution
     * representing the input to analyze.
     */
    static pair<double, double> simulate( const vector<double> & all_watch_times,
            default_random_engine & prng,
            const SchemeStats & /* real */scheme ) {
        /* step 1: draw a random watch duration */ 
        uniform_int_distribution<> possible_watch_time_index(0, all_watch_times.size() - 1); 
        const double simulated_watch_time = all_watch_times.at(possible_watch_time_index(prng)); 

        /* step 2: draw a stall ratio for the scheme from a similar observed watch time */
        unsigned int simulated_watch_time_binned = SchemeStats::watch_time_bin(simulated_watch_time);
        size_t num_stall_ratio_samples = scheme.binned_stall_ratios.at(simulated_watch_time_binned).size();

        if (num_stall_ratio_samples > 0) {  
            // scheme has nonempty bin corresponding to the simulated watch time => 
            // draw stall ratio from that bin
            uniform_int_distribution<> possible_stall_ratio_index(0, num_stall_ratio_samples - 1);
            const double simulated_stall_time = simulated_watch_time * scheme.binned_stall_ratios.at(simulated_watch_time_binned).at(possible_stall_ratio_index(prng));

            return {simulated_watch_time, simulated_stall_time};
        } else {
            if (simulated_watch_time_binned == 0) { // watch_time_bin already throws if bin < 2
                throw runtime_error("no small session stall_ratios?");
            }
            
            /* If simulated bin is empty, draw from aggregate over left and right bins; throw if both empty */
            const size_t left_num_stall_ratio_samples = simulated_watch_time_binned > scheme.MIN_BIN ? 
                                                        scheme.binned_stall_ratios.at(simulated_watch_time_binned - 1).size() 
                                                        : 0;
            const size_t right_num_stall_ratio_samples = simulated_watch_time_binned < scheme.MAX_BIN ? 
                                                        scheme.binned_stall_ratios.at(simulated_watch_time_binned + 1).size() 
                                                        : 0;
            if (left_num_stall_ratio_samples == 0 && right_num_stall_ratio_samples == 0) {
                throw runtime_error("no nonempty bins from which to draw stall_ratio");
            }

            uniform_int_distribution<> agg_possible_stall_ratio_index(0, left_num_stall_ratio_samples + right_num_stall_ratio_samples - 1); 
            const unsigned agg_stall_ratio_index = agg_possible_stall_ratio_index(prng);
            unsigned stall_ratio_index;   // index relative to nsamples in chosen bin
            if (agg_stall_ratio_index >= left_num_stall_ratio_samples) {    // right bin
                simulated_watch_time_binned++;
                stall_ratio_index = agg_stall_ratio_index - left_num_stall_ratio_samples;
            } else {                                                        // left bin
                simulated_watch_time_binned--; 
                stall_ratio_index = agg_stall_ratio_index;
            }
            const double simulated_stall_time = simulated_watch_time * scheme.binned_stall_ratios.at(simulated_watch_time_binned).at(stall_ratio_index);

            return {simulated_watch_time, simulated_stall_time};
        }
    }

    /* For each sample in (real) scheme, take a simulated sample 
     * Return resulting simulated total stall ratio */
    static double simulate_realization( const vector<double> & all_watch_times,
            default_random_engine & prng,
            const SchemeStats & /* real */scheme ) {
        SchemeStats scheme_simulated;

        for ( unsigned int i = 0; i < scheme.samples; i++ ) {
            const auto [watch_time, stall_time] = simulate(all_watch_times, prng, scheme);
            scheme_simulated.add_sample(watch_time, stall_time);	    
        }

        return scheme_simulated.observed_stall_ratio();
    }

    class Realizations {
        string _name;
        // simulated stall ratios 
        vector<double> _stall_ratios{};
        // real (non-simulated) stats
        SchemeStats _scheme_sample;

        public:
        Realizations( const string & name, const SchemeStats & scheme_sample ) : _name(name), _scheme_sample(scheme_sample) {}

        void add_realization( const vector<double> & all_watch_times,
                default_random_engine & prng ) {
            _stall_ratios.push_back(simulate_realization(all_watch_times, prng, _scheme_sample));   // pass in real stats
        }

        // mean and 95% confidence interval of *simulated* stall ratios
        tuple<double, double, double> stats() {
            sort(_stall_ratios.begin(), _stall_ratios.end());

            const double lower_limit = _stall_ratios[.025 * _stall_ratios.size()];
            const double upper_limit = _stall_ratios[.975 * _stall_ratios.size()];

            const double total = accumulate(_stall_ratios.begin(), _stall_ratios.end(), 0.0);
            const double mean = total / _stall_ratios.size();

            return { lower_limit, mean, upper_limit };
        }

        void print_samplesize() const {
            cout << fixed << setprecision(3);
            cout << "#" << _name << " considered " << _scheme_sample.samples << " sessions, stall/watch hours: " << _scheme_sample.total_stall_time / 3600.0 << "/" << _scheme_sample.total_watch_time / 3600.0 << "\n";
        }

        void print_summary() {
            const auto [ lower_limit, mean, upper_limit ] = stats();
            const auto [ lower_ssim_limit, mean_ssim, upper_ssim_limit ] = _scheme_sample.sem_ssim();
            const auto [ lower_ssim_variation, mean_ssim_variation, upper_ssim_variation ] = _scheme_sample.sem_ssim_variation();

            cout << fixed << setprecision(8);
            cout << _name << " stall ratio (95% CI): " << 100 * lower_limit << "% .. " << 100 * upper_limit << "%, mean= " << 100 * mean;
            cout << "; SSIM (95% CI): " << lower_ssim_limit << " .. " << upper_ssim_limit << ", mean= " << mean_ssim;
            cout << "; SSIMvar (95% CI): " << lower_ssim_variation << " .. " << upper_ssim_variation << ", mean= " << mean_ssim_variation;
            cout << "\n";
        }
    };

    /* For each scheme: simulate stall ratios, and calculate stall ratio mean/CI over simulated samples.
     * Calculate SSIM and SSIMvar mean/CI over real samples. */
    void do_point_estimate() {
        random_device rd;
        default_random_engine prng(rd());

        // initialize with real stats, from which to sample
        constexpr unsigned int iteration_count = 10000;
        vector<Realizations> realizations;
        for (const auto & [desired_scheme, desired_scheme_stats] : scheme_stats) {
            realizations.emplace_back(Realizations{desired_scheme, desired_scheme_stats});
        }

        /* For each scheme, take 10000 simulated stall ratios */
        for (unsigned int i = 0; i < iteration_count; i++) {
            if (i % 10 == 0) {
                cerr << "\rsample " << i << "/" << iteration_count << "                    ";
            }

            for (auto & realization : realizations) {
                realization.add_realization(all_watch_times, prng);
            }
        }
        cerr << "\n";

        /* report statistics */
        for (const auto & realization : realizations) {
            realization.print_samplesize();
        }
        for (auto & realization : realizations) {
            realization.print_summary();
        }
    }
};

void confint_main(const string & intersection_filename, bool slow_sessions) {
    Statistics stats {  intersection_filename };
    stats.parse_stdin(slow_sessions);
    stats.do_point_estimate(); 
}

// TODO: update file comment
void print_usage(const string & program) {
    cerr << "Usage: " << program << " --scheme-intersection <intersection_filename> --session-speed <session_speed>\n" 
            "intersection_filename: Output of schemedays --intersect-schemes --intersect-outfile, "
            "containing desired schemes and the days they intersect.\n"
            "session_speed: slow or all\n";
}

int main(int argc, char *argv[]) {
   try {
        if (argc < 1) {
            abort();
        }
        const option opts[] = {
            {"scheme-intersection", required_argument, nullptr, 'i'},
            {"session-speed", required_argument, nullptr, 's'},
            {nullptr, 0, nullptr, 0}
        };
        string intersection_filename;
        string session_speed;

        while (true) {
            const int opt = getopt_long(argc, argv, "i:s:", opts, nullptr);
            if (opt == -1) break;
            switch (opt) {
                case 'i': 
                    intersection_filename = optarg;
                    break;
                case 's':
                    session_speed = optarg;
                    break;
                default:
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
            }
        }

        if (optind != argc) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if (intersection_filename.empty() or (session_speed != "slow" and session_speed != "all")) {
            cerr << "Error: Scheme days file and session speed (slow or all) are required\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        bool slow_sessions = session_speed == "slow";
        confint_main(intersection_filename, slow_sessions); 
        
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
