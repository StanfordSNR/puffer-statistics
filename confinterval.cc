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

    using day_range = pair<uint64_t, uint64_t>;

    /* For each scheme, records day range(s) the scheme ran
     * If a range has only one day in it, start and end of range are equal.
     * For each scheme, the ranges are discontiguous, nonoverlapping, and sorted.
     * e.g. if MPC ran Jan 1-June 1 and Oct 12, record
     * MPC => { [Jan 1, June 1], [Oct 12, Oct 12] } */
    map<string, vector<day_range>> scheme_days{};
    /* Whether to write scheme days to file */
    bool store_scheme_days;
    /* File storing scheme_days */
    string scheme_days_filename;
    /* Days to be analyzed (i.e. intersection of all days the requested
     * schemes were run, according to scheme_days) */
    vector<day_range> acceptable_days{};

    // real (non-simulated) stats 
    map<string, SchemeStats> scheme_stats{};

    // analyze slow sessions only
    bool slow_sessions;

    public: 
     Statistics (bool store_scheme_days, const string & scheme_group,
                 const string & scheme_days_filename, bool slow_sessions): 
                 store_scheme_days(store_scheme_days),
                 scheme_days_filename(scheme_days_filename),
                 slow_sessions(slow_sessions) {
        if (not store_scheme_days) {
            /* Read scheme days map from file, and find list of intersecting
             * days to be analyzed */
            read_scheme_days();
            vector<string> requested_schemes;
            if (scheme_group == "primary") {
                 requested_schemes = 
                    {"puffer_ttp_cl/bbr", "mpc/bbr", 
                     "robust_mpc/bbr", "pensieve/bbr", "linear_bba/bbr"};
            } else if (scheme_group == "vintages") {
                requested_schemes = 
                    {"puffer_ttp_cl/bbr", "puffer_ttp_20190202/bbr", 
                     "puffer_ttp_20190302/bbr", "puffer_ttp_20190402/bbr", "puffer_ttp_20190502/bbr"};
            }
            acceptable_days = scheme_days_intx(requested_schemes); 
            // Populate scheme_stats, so parse() can determine if a scheme was requested
            for (const string & scheme : requested_schemes) {
                scheme_stats[scheme] = SchemeStats{};
            }
        }
    }

    /* Find intx of two date ranges, if any */
    optional<day_range> range_range_intx(day_range a, day_range b) {
        const uint64_t max_low = max(get<0>(a), get<0>(b));
        const uint64_t min_high = min(get<1>(a), get<1>(b));
        if (max_low > min_high) return {};
        return pair{max_low, min_high};
    }

    /* Find intx of two lists of ranges, if any */
    const vector<day_range> rangelist_rangelist_intx(const vector<day_range> a, const vector<day_range> b) {
        vector<day_range> total_intx;
        // Total intx is sum of each range-range intx
        for (const day_range & a_range : a) { 
            for (const day_range & b_range : b) {
                optional<day_range> _range_range_intx = range_range_intx(a_range, b_range);
                if (_range_range_intx) {
                    total_intx.emplace_back(_range_range_intx.value());
                }
            }
        }
        return total_intx;
    }
    /* Find intx of one range with one scheme's ranges, if any */
    const vector<day_range> range_scheme_intx(day_range range, const string & scheme) {
        // use rangelist-rangelist intx with singleton list
        return rangelist_rangelist_intx(vector<day_range>{range}, scheme_days[scheme]); 
    }

    /* Finds intx of given range across *all* given schemes */
    const vector<day_range> range_allschemes_intx(day_range range, const vector<string> & schemes) {
        vector<day_range> running_intx{range};
        // For each other scheme, intx with running range-scheme intx
        for (const string & scheme : schemes) {   
            const vector<day_range> & _range_scheme_intx = range_scheme_intx(range, scheme); 
            running_intx = rangelist_rangelist_intx(_range_scheme_intx, running_intx);
        }
        return running_intx;
    }

    /* Given a list of schemes, find the days where all schemes in the list were run, according to scheme_days.
     * Definitely not optimized (e.g. doesn't take much advantage of scheme_days being sorted), 
     * but we expect only a few ranges per scheme. */
    const vector<day_range> scheme_days_intx(const vector<string> schemes) {
        // TODO: this is called from Statistics constructor, so maybe throwing is not the best...
        if (scheme_days.empty()) {
            throw runtime_error("Scheme days must be read from file before calculating list of days to analyze");
        }
        vector<day_range> total_intx;
        // For each range in the first scheme, add its intx with all the other schemes to total_intx
        const vector<string> other_schemes = vector<string>(schemes.begin() + 1, schemes.end());
        for (const day_range & range : scheme_days[schemes.front()]) {
            const vector<day_range> & _range_allschemes_intx = range_allschemes_intx(range, other_schemes);
            total_intx.insert(total_intx.end(), make_move_iterator(_range_allschemes_intx.begin()),
                                                make_move_iterator(_range_allschemes_intx.end()));
        }
        /* TODO: trace what happens if empty -- maybe just empty graph?
        if (total_intx.empty()) {
            throw runtime_error("requested schemes were not run on any intersecting days");
        }
        */
        return total_intx;
    }

    void test_scheme_days_intx() {
        if (not scheme_days.empty()) {
            cerr << "scheme_days is populated; aborting test" << endl;
            return;
        }
        bool passed;
        scheme_days["A"] = {{1,4}, {6,7}};
        scheme_days["B"] = {{0,1}, {5,8}};
        passed = scheme_days_intx({"A", "B"}) == vector<day_range>{{1,1}, {6,7}};
        assert(passed);
        passed = scheme_days_intx({"A"}) == vector<day_range>{{1,4}, {6,7}};
        assert(passed);

        scheme_days.clear();
        scheme_days["A"] = {{0,2}, {5,8}};
        scheme_days["B"] = {{2,6}, {8,10}};
        scheme_days["C"] = {{1,3}, {6,7}};
        passed = scheme_days_intx({"A", "B", "C"}) == vector<day_range>{{2,2}, {6,6}};
        assert(passed);
        scheme_days.clear();
    }

    /* Given the base timestamp and scheme of a stream, add 
     * corresponding day to the list of days each scheme was run.
     * Assumes input to confinterval is sorted by day, 
     * so we only ever add to the end of the list
     * (OK if not sorted within day, although analyze does that anyway) */
    void record_scheme_days(uint64_t ts, const string & scheme) {
        const unsigned sec_per_day = 60 * 60 * 24;
        // ts_day and max_day are rounded down to nearest day
        const uint64_t ts_day = ts / sec_per_day * sec_per_day;
        const optional<uint64_t> max_day = scheme_days[scheme].empty() ? nullopt 
                                           : make_optional(get<1>(scheme_days[scheme].back()));   
        /* TODO: remove
        cout << "recording day " << ts_day << " for scheme " << scheme <<
            "; max day: " << max_day.value_or(-1) << endl;   
        */
        if (max_day and ts_day < max_day.value()) {
            throw runtime_error("confinterval received out-of-order day");
        }

        if (not max_day or ts_day - max_day.value() > sec_per_day) { 
           // new day creates hole => add new range 
           scheme_days[scheme].push_back({ts_day, ts_day});   // no end date yet
        } else if (ts_day - max_day.value() == sec_per_day) {
           // new day is contiguous => extend existing range by a day
           get<1>(scheme_days[scheme].back()) += sec_per_day; 
        }
        // if ts_day == max_day, already recorded this day
    }
    
    /* Read scheme days from filename into scheme_days map for later
     * lookup of timestamps to be analyzed */
    void read_scheme_days() {
        ifstream scheme_days_file{ scheme_days_filename };
        if (not scheme_days_file.is_open()) {
            throw runtime_error( "can't open " + scheme_days_filename );
        }

        string line_storage;
        string scheme;
        char scratch;
        uint64_t range_start, range_end;

        while (getline(scheme_days_file, line_storage)) {
            if (line_storage.empty()) continue;
            istringstream line(line_storage);
            if (!(line >> skipws >> scheme)) {
                throw runtime_error("error reading scheme from " + scheme_days_filename); 
            }
            while (not line.eof()) {    // read all ranges
                if (!(line >> range_start) || !(line >> scratch) || !(line >> range_end)) {
                    throw runtime_error("error reading dates from " + scheme_days_filename); 
                }
                scheme_days[scheme].emplace_back(pair{range_start, range_end});
            }
        } 
        if (scheme_days_file.bad()) {
            throw runtime_error("error reading " + scheme_days_filename);
        }
        // TODO: remove
        cout << "scheme days read from file: " << endl;
        for (const auto & [scheme, ranges] : scheme_days) {
            cout << "\n" << scheme;
            for (const auto & [range_start, range_end] : ranges) {
                cout << range_start << ":" << range_end << " "; 
            }
        }
    }

    /* Write map of scheme days to file. */
    void write_scheme_days() {
        ofstream scheme_days_file;
        scheme_days_file.open(scheme_days_filename);
        if (not scheme_days_file.is_open()) {
            throw runtime_error( "can't open " + scheme_days_filename);
        }
        // file line format
        // Puffer 1565193009:1567206883 1567206884:1567206885 ...
        for (const auto & [scheme, ranges] : scheme_days) {
            scheme_days_file << scheme;
            for (const auto & range : ranges) {
                scheme_days_file << " " << get<0>(range) << ":" << get<1>(range);
            }
            scheme_days_file << "\n";
        }
    }

    /* Indicates whether ts is within the range of acceptable days, calculated as
     * the intersection of the days that all requested schemes were run. */
    bool ts_is_acceptable(uint64_t ts) {
        bool acceptable = false;
        // acceptable_days is discontiguous, nonoverlapping, and sorted
        for (const auto & [range_start, range_end] : acceptable_days) {
            if (ts >= range_start && ts <= range_end) {
                acceptable = true;
                break;
            }
            // if ts is less than current range's end, will also be less than next range's start
            if (ts < range_end) break;  
        }
        return acceptable;
    }
    
    /* Populate SchemeStats with per-scheme watch/stall/ssim, 
     * ignoring stream if stream is bad/outside study period/short watch time.
     * Record all watch times independent of scheme. 
     * Record days each scheme was run, if desired. */
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

            const auto & [ts_str, goodbad, fulltrunc, badreason, scheme, ip, os, channelchange, init_id,
                  extent, usedpct, mean_ssim, mean_delivery_rate, average_bitrate, ssim_variation_db,
                  startup_delay, time_after_startup,
                  time_stalled]
                      = tie(fields[0], fields[1], fields[2], fields[3],
                              fields[4], fields[5], fields[6], fields[7],
                              fields[8], fields[9], fields[10], fields[11],
                              fields[12], fields[13], fields[14], fields[15], fields[16], fields[17]);

            const uint64_t ts = to_uint64(ts_str);
            if (store_scheme_days) {
                record_scheme_days(ts, string(scheme));
                /* If writing scheme days to file, don't calculate conf int 
                 * since we don't have a timestamp range */
                continue;   
            }
            if (not ts_is_acceptable(ts)) {
                cout << "skipping out-of-range ts: " << ts << endl; // TODO: remove
                continue;
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

        if (store_scheme_days) {
            write_scheme_days();
        }
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
        for (const auto & [requested_scheme, requested_scheme_stats] : scheme_stats) {
            realizations.emplace_back(Realizations{requested_scheme, requested_scheme_stats});
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

void confint_main(bool store_scheme_days, const string & scheme_group, 
                  const string & scheme_days_filename, bool slow_sessions) {
    Statistics stats { store_scheme_days, scheme_group, scheme_days_filename, slow_sessions };
    stats.test_scheme_days_intx(); // TODO: remove
    stats.parse_stdin();
    // stats.do_point_estimate(); // TODO: put back!
}

void print_usage(const string & program) {
    cerr << "Usage: " << program << " <scheme_group> <scheme_days_action> <scheme_days_filename> <session_speed>\n" 
            "scheme_group: Either primary or vintages (for now)\n"
            "scheme_days_action: Either --read-scheme-days or --write-scheme-days.\n"
            "Scheme_days is a list of days each scheme has run.\n"
            "To read this list from an already populated file, select read.\n"
            "To form this list from the input data and write to file for later use, select write.\n"
            "(No confidence intervals will be calculated if write is selected, since scheme days is needed to determine the dates to analyze).\n"
            "scheme_days_filename: File from which to read or write scheme_days.\n"
            "session_speed: slow-sessions or all-sessions\n";
}

int main(int argc, char *argv[]) { 
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc < 2) { 
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        bool store_scheme_days = false;
        bool slow_sessions = false;
        const option longopts[] = {
            {"store-scheme-days", no_argument, nullptr, 'd'},
            {"slow-sessions", no_argument, nullptr, 's'},
            {nullptr, 0, nullptr, 0}
        };

        while (true) {
            const int opt = getopt_long(argc, argv, "ds", longopts, nullptr);
            if (opt == -1) {
                break;
            }
            if (opt == 'd') {
                store_scheme_days = true;
            } else if (opt == 's') {
                slow_sessions = true;
            } else {
                print_usage(argv[0]);
                return EXIT_FAILURE;
            }
        }
        if (optind != argc - 2) {   
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        // TODO: allow arbitrary list of schemes
        string scheme_group = argv[optind++];
        if (scheme_group != "primary" && scheme_group != "vintages") {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        string scheme_days_filename = argv[optind];
        confint_main(store_scheme_days, scheme_group, scheme_days_filename, slow_sessions); 
        
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
