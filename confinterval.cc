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
 * To stdout, outputs each scheme's mean stall ratio, SSIM, and SSIM variance,
 * along with confidence intervals. 
 * Takes as mandatory arguments the file containing desired schemes and the days they intersect 
 * (from pre_confinterval --intersect-outfile), and the name of the watch times files 
 * (from pre_confinterval --build-watchtimes-list).
 * Stall ratio is calculated over simulated samples;
 * SSIM/SSIMvar is calculated over real samples.
 */

/* 
 * Takes as optional argument a date range, restricting consideration to that range 
 * even if scheme-intersection and/or input data contain additional days.
 * That is, the set of days considered from the input data is the 
 * intersection of scheme-intersection and date range.
 * If no date range supplied, all days in scheme-intersection are considered.
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
     // Stall ratio data from *real* distribution
    array<vector<double>, MAX_N_BINS> binned_stall_ratios{};

    unsigned int samples = 0;
    double total_watch_time = 0;
    double total_stall_time = 0;    

    // SSIM data from *real* distribution
    vector<pair<double,double>> ssim_samples{};
    vector<double> ssim_variation_samples{};

    double total_ssim_watch_time = 0;

    /* Given watch time in seconds, return bin index as 
     * log(watch time), if watch time : [2^MIN_BIN, 2^MAX_BIN] (else, throw) */
    static unsigned int watch_time_bin(const double raw_watch_time) {
        const unsigned int watch_time_bin = lrintf(floorf(log2(raw_watch_time)));    
        if (watch_time_bin < MIN_BIN or watch_time_bin > MAX_BIN) {
            throw std::runtime_error("watch time bin out of range");
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
    vector<double> watch_times{}; 

    /* Whenever a timestamp is used to represent a day, round down to Influx backup hour,
     * in seconds (analyze records ts as seconds) */
    using Day_sec = uint64_t;

    /* Days listed in the intersection file */
    set<Day_sec> days_from_intx{};

    /* Days specified in argument to program (empty if --days not supplied) */
    optional<pair<Day_sec, Day_sec>> days_from_arg{};
    
    // real (non-simulated) stats 
    map<string, SchemeStats> scheme_stats{};

    public:     
     Statistics (const string & intersection_filename, const string & watch_times_filename,
                 const string & stream_speed, optional<pair<Day_sec, Day_sec>> days_from_arg) : 
        days_from_arg(days_from_arg) {
        vector<string> desired_schemes;
        /* Read file containing desired schemes, and list of days they intersect */
        read_intersection_file(intersection_filename, desired_schemes);
        // Initialize scheme_stats, so parse() knows the desired schemes
        for (const string & scheme : desired_schemes) {
            scheme_stats[scheme] = SchemeStats{};
        }
        /* Read file containing watch times */
        read_watch_times_file(watch_times_filename, stream_speed);
        
        // Log schemes and days for convenience
        cerr << "Schemes:\n";
        for (const auto & desired_scheme : desired_schemes) {
            cerr << desired_scheme << " ";
        }
        cerr << "\nDays from intersect-outfile:\n";  
        print_intervals(days_from_intx);
        if (days_from_arg) {
            cerr << "\nDays from --days argument:\n"
                 << days_from_arg.value().first << ":" << days_from_arg.value().second << "\n";
        }
    }

     void read_intersection_file(const string & intersection_filename, vector<string> & desired_schemes) {
        ifstream intersection_file{intersection_filename};
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
        Day_sec day;
        istringstream days_line(line_storage);
        while (days_line >> day) {  
            days_from_intx.emplace(day);
        }
        intersection_file.close();
        if (intersection_file.bad()) {
            throw runtime_error("error reading " + intersection_filename);
        }
     }
        
     void read_watch_times_file(const string & watch_times_filename,
                                const string & stream_speed) {
        string full_watch_times_filename;
        /* watch_times_filename may be a path, e.g ../watch_times_out.txt */
        size_t slash_pos = watch_times_filename.rfind('/');
        if (slash_pos != string::npos) {     
            full_watch_times_filename = watch_times_filename.substr(0, slash_pos + 1) + 
                                        stream_speed + "_" + watch_times_filename.substr(slash_pos + 1);
        } else {
            full_watch_times_filename = stream_speed + "_" + watch_times_filename;
        }
        ifstream watch_times_file{full_watch_times_filename};
        if (not watch_times_file.is_open()) {
            throw runtime_error( "can't open " + full_watch_times_filename);
        }
        string line_storage;
        double watch_time;
        if (!getline(watch_times_file, line_storage)) {
            throw runtime_error("error reading " + full_watch_times_filename);
        }
        istringstream line(line_storage);
        while (line >> watch_time) {
            watch_times.emplace_back(watch_time);
        }

        watch_times_file.close();
        if (watch_times_file.bad()) {
            throw runtime_error("error reading " + full_watch_times_filename);
        }
        // shuffle watch times before sampling
        random_shuffle(watch_times.begin(), watch_times.end());
     }
    
    /* Indicates whether ts belongs to an "acceptable" day, 
     * i.e. a day listed in the intersection file
     * and in range specified by the --days argument (if supplied) */
    bool ts_is_acceptable(uint64_t ts) {
        Day_sec day = ts2Day_sec(ts);
        bool in_arg_range = true;
        if (days_from_arg) {  // --days argument was supplied
            in_arg_range = day >= days_from_arg.value().first and 
                           day <= days_from_arg.value().second;
        }
        return days_from_intx.count(day) and in_arg_range;
    }
    
    /* Populate SchemeStats with per-scheme watch/stall/ssim, 
     * ignoring stream if stream is bad/outside study period/short watch time.
     * Record all watch times independent of scheme. 
     * Record days each scheme was run, if desired. */
    void parse_stdin(const string & stream_speed) {
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

            split_on_char(timestamp, '=', scratch);
            if (scratch[0] != "ts"sv) {
                throw runtime_error("timestamp field mismatch");
            }

            const uint64_t ts = to_uint64(scratch[1]);

            if (not ts_is_acceptable(ts)) {
                continue;
            } 

            if (stream_speed == "slow") {
                split_on_char(mean_delivery_rate, '=', scratch);
                if (scratch[0] != "mean_delivery_rate"sv) {
                    throw runtime_error("delivery rate field mismatch");
                }
                const double delivery_rate = to_double(scratch[1]); 
                if (not stream_is_slow(delivery_rate)) {
                    continue;
                }
            }
            split_on_char(time_after_startup, '=', scratch);
            if (scratch[0] != "total_after_startup"sv) {
                throw runtime_error("watch time field mismatch");
            }

            const double watch_time = to_double(scratch[1]);

            if (watch_time < (1 << MIN_BIN)) {
                continue;
            }

            split_on_char(time_stalled, '=', scratch);
            if (scratch[0] != "stall_after_startup"sv) {
                throw runtime_error("stall time field mismatch");
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
            
            split_on_char(goodbad, '=', scratch);
            if (scratch[0] != "valid"sv) {
                throw runtime_error("validity field mismatch");
            }

            string_view validity = scratch[1];
            // EXCLUDE BAD (but not trunc)
            if (validity == "bad"sv) {  
                continue;
            }
            
            split_on_char(scheme, '=', scratch);
            if (scratch[0] != "scheme"sv) {
                throw runtime_error("scheme field mismatch");
            }

            string_view schemesv = scratch[1];

            // Record stall ratio, ssim, ssim variation 
            // Ignore if not one of the requested schemes 
            SchemeStats *the_scheme = nullptr;
            auto found_scheme = scheme_stats.find(string(schemesv));
            if (found_scheme != scheme_stats.end()) {
                the_scheme = &found_scheme->second;
            }

            if (the_scheme) {
                the_scheme->add_sample(watch_time, stall_time);
                if ( mean_ssim_val >= 0 ) { the_scheme->add_ssim_sample(watch_time, mean_ssim_val); }
                // SSIM variation = 0 over a whole stream is questionable
                if ( ssim_variation_db_val > 0 and ssim_variation_db_val <= 10000 ) { 
                    the_scheme->add_ssim_variation_sample(ssim_variation_db_val); 
                }
            }
        }   // end while
    }

    /* Draw from aggregate over the pair of neighbor bins nhops away from the simulated watch time on each side
     * (e.g. the direct left and right bins, if nhops == 1). */
    static optional<double> draw_from_neighbor_bins(double simulated_watch_time, unsigned nhops,
                                                    default_random_engine & prng,
                                                    const SchemeStats & /* real */ scheme ) {
        
        unsigned int simulated_watch_time_binned = SchemeStats::watch_time_bin(simulated_watch_time);
       
        if (nhops > MAX_BIN - MIN_BIN) {
            throw logic_error("Attempted to draw from pair of bins " + to_string(nhops) + " away from bin " +
                              to_string(simulated_watch_time_binned) + 
                              ". Valid bins (inclusive): " + to_string(MIN_BIN) + ":" + to_string(MAX_BIN));
        }
        
        // unused if neighbor is out of range, since num_samples will be 0 
        unsigned int left_neighbor = simulated_watch_time_binned - nhops;    
        unsigned int right_neighbor = simulated_watch_time_binned + nhops;
        const size_t left_num_stall_ratio_samples = simulated_watch_time_binned < MIN_BIN + nhops ? 
                                                    0 :
                                                    scheme.binned_stall_ratios.at(left_neighbor).size();
        const size_t right_num_stall_ratio_samples = simulated_watch_time_binned > MAX_BIN - nhops ?
                                                    0 :
                                                    scheme.binned_stall_ratios.at(right_neighbor).size();
        
        if (left_num_stall_ratio_samples == 0 && right_num_stall_ratio_samples == 0) {
            /* Both neighbors empty. Do not throw -- caller may repeat with a larger nhops. */
            return {};
        }

        uniform_int_distribution<> agg_possible_stall_ratio_index(0, left_num_stall_ratio_samples + 
                                                                     right_num_stall_ratio_samples - 1); 
        const unsigned agg_stall_ratio_index = agg_possible_stall_ratio_index(prng);
        unsigned stall_ratio_index, selected_neighbor;   // stall_ratio_index: relative to nsamples in chosen bin
        
        if (agg_stall_ratio_index >= left_num_stall_ratio_samples) {    // right bin
            selected_neighbor = right_neighbor;
            stall_ratio_index = agg_stall_ratio_index - left_num_stall_ratio_samples;
        } else {                                                        // left bin
            selected_neighbor = left_neighbor;
            stall_ratio_index = agg_stall_ratio_index;
        }
        const double simulated_stall_time = simulated_watch_time * 
            scheme.binned_stall_ratios.at(selected_neighbor).at(stall_ratio_index);
        
        return simulated_stall_time;
    }

    /* Simulate watch and stall time: 
     * Draw a random watch time from all watch times; 
     * draw a stall ratio from the bin corresponding to the simulated watch time, 
     * in the per-scheme stall ratio distribution
     * representing the input to analyze.
     */
    static pair<double, double> simulate(const vector<double> & watch_times,
                                         default_random_engine & prng,
                                         const SchemeStats & /* real */ scheme ) {
        /* step 1: draw a random watch time from static watch times samples */ 
        uniform_int_distribution<> possible_watch_time_index(0, watch_times.size() - 1);
        const double simulated_watch_time = watch_times.at(possible_watch_time_index(prng));

        /* step 2: draw a stall ratio for the scheme from a similar observed watch time */
        unsigned int simulated_watch_time_binned = SchemeStats::watch_time_bin(simulated_watch_time);
        
        size_t num_stall_ratio_samples = scheme.binned_stall_ratios.at(simulated_watch_time_binned).size();

        if (num_stall_ratio_samples > 0) {  
            // scheme has nonempty bin corresponding to the simulated watch time => 
            // draw stall ratio from that bin
            uniform_int_distribution<> possible_stall_ratio_index(0, num_stall_ratio_samples - 1);
            // multiply stall ratio by un-binned simulated watch time, since stall ratio uses un-binned real watch time
            const double simulated_stall_time = simulated_watch_time * 
                scheme.binned_stall_ratios.at(simulated_watch_time_binned).at(possible_stall_ratio_index(prng));

            return {simulated_watch_time, simulated_stall_time};
        } else {
            unsigned nhops = 1; 
            optional<double> simulated_stall_time;
            while (not (simulated_stall_time = draw_from_neighbor_bins(simulated_watch_time, nhops++, prng, scheme))) {
                /* Draw from aggregate over the pair of bins one hop away, two hops, etc until finding a non-empty bin.
                 * Should always terminate, since at least one bin in the distribution should be non-empty 
                 * (but draw_from_neighbor_bins checks just in case) */
            }

            return {simulated_watch_time, simulated_stall_time.value()};
        }
    }

    /* For each sample in (real) scheme, take a simulated sample 
     * Return resulting simulated total stall ratio */
    static double simulate_realization( const vector<double> & watch_times,
                                        default_random_engine & prng,
                                        const SchemeStats & /* real */scheme ) {
        SchemeStats scheme_simulated;

        for ( unsigned int i = 0; i < scheme.samples; i++ ) {
            const auto [watch_time, stall_time] = simulate(watch_times, prng, scheme);
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

        void add_realization( const vector<double> & watch_times, 
                              default_random_engine & prng ) {
            _stall_ratios.push_back(simulate_realization(watch_times, prng, _scheme_sample));   // pass in real stats
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
            cout << "#" << _name << " considered " << _scheme_sample.samples << " streams, stall/watch hours: " 
                 << _scheme_sample.total_stall_time / 3600.0 << "/" << _scheme_sample.total_watch_time / 3600.0 
                 << "\n";
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
                realization.add_realization(watch_times, prng);
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

void confint_main(const string & intersection_filename, const string & watch_times_filename,
                  const string & stream_speed, optional<pair<Day_sec, Day_sec>> days_from_arg) {
    Statistics stats {intersection_filename, watch_times_filename, stream_speed, days_from_arg};
    stats.parse_stdin(stream_speed);
    stats.do_point_estimate(); 
}

void print_usage(const string & program) {
    cerr << "Usage: " << program 
         << " --scheme-intersection <intersection_filename>"
            " --stream-speed <stream_speed>"
            " --watch-times <watch_times_filename_postfix>\n"
            "intersection_filename: Output of pre_confinterval --intersect-schemes --intersect-outfile, "
            "containing desired schemes and the days they intersect.\n"
            "stream_speed: slow or all\n"
            "watch_times_filename_postfix: Output of pre_confinterval --build-watch_times-list, "
            "containing watch times (specified stream_speed will be prepended).\n"
            "Optionally, --days <date_range>\n"
            "date_range: Inclusive range of dates to consider "
            "[e.g. 2019-07-01T11_2019-07-02T11:2019-07-04T11_2019-07-05T11]\n";
    // TODO: throw if requested date not in intersection_filename 
    // (i.e. pre_confinterval was run on the wrong days)?
}

int main(int argc, char *argv[]) {
   try {
        if (argc < 1) {
            abort();
        }
        const option opts[] = {
            {"scheme-intersection", required_argument, nullptr, 'i'},
            {"stream-speed", required_argument, nullptr, 's'},
            {"watch-times", required_argument, nullptr, 'w'},
            {"days", required_argument, nullptr, 'd'},
            {nullptr, 0, nullptr, 0}
        };
        string intersection_filename, watch_times_filename,
               stream_speed;
        
        optional<pair<Day_sec, Day_sec>> days_from_arg;

        while (true) {
            const int opt = getopt_long(argc, argv, "i:s:w:d:", opts, nullptr);
            if (opt == -1) break;
            switch (opt) {
                case 'i': 
                    intersection_filename = optarg;
                    break;
                case 's':
                    stream_speed = optarg;
                    if (stream_speed != "slow" and stream_speed != "all") {
                        cerr << "Error: Stream speed must be \"slow\" or \"all\"\n\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    break;
                case 'w':
                    watch_times_filename = optarg;
                    break;
                case 'd':
                    {
                    /* Parse days argument to timestamps */
                    istringstream days_stream(optarg);
                    string start_day_str, end_day_str; 
                    // str2Day returns empty if parse fails
                    getline(days_stream, start_day_str, ':');
                    optional<Day_sec> start_ts = str2Day_sec(start_day_str);
                    getline(days_stream, end_day_str);
                    optional<Day_sec> end_ts = str2Day_sec(end_day_str);
                    
                    if (not start_ts or not end_ts) {
                        cerr << "Date argument could not be parsed\n";
                        print_usage(argv[0]);
                        return EXIT_FAILURE;
                    }
                    /* Make end_ts inclusive; e.g. if arg is 
                     * 2019-07-01T11_2019-07-02T11:2019-07-04T11_2019-07-05T11, then
                     * end_ts = 2019-07-05 11AM (not 2019-07-04 11AM) */
                    days_from_arg = pair{start_ts.value(), end_ts.value() + 60 * 60 * 24};
                    break; 
                    }
                default:
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
            }
        }

        if (optind != argc) {
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }
        if (intersection_filename.empty() or watch_times_filename.empty() or stream_speed.empty()) {
            cerr << "Error: Scheme days file, watch time file, and stream speed are required\n\n";
            print_usage(argv[0]);
            return EXIT_FAILURE;
        }

        confint_main(intersection_filename, watch_times_filename, stream_speed, days_from_arg); 
        
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

