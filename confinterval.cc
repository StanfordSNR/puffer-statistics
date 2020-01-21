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
    vector<double> all_watch_times{};

    // real (non-simulated) stats
    SchemeStats puffer{};
    SchemeStats mpc{}, robust_mpc{}, pensieve{}, bba{};

    public:
    /* Populate SchemeStats with per-scheme watch/stall/ssim, 
     * ignoring stream if stream is bad/outside study period/short watch time.
     * Record all watch times independent of scheme. */
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
            
            if ((ts >= 1547884800 and ts < 1565193009) or (ts > 1567206883)) {  
                // analyze this period: January 19-August 7, and August 30 onward
                // this is the primary study period
                /* Note input to confinterval needs to include raw data from days on either side of each endpoint,
                 * i.e. 2019-01-18T11_2019-01-19T11 to 2019-08-07T11_2019-08-08T11 
                 * and 2019-08-29T11_2019-08-30T11 to 2019-09-11T11_2019-09-12T11,
                 * in order to reproduce paper */
            } else {
                continue;
            }

            /* 
             * This gives figure 8b (slow sessions), not 8a
            split_on_char(mean_delivery_rate, '=', scratch);
            if (scratch[0] != "mean_delivery_rate"sv) {
                throw runtime_error("field mismatch");
            }
            const double delivery_rate = to_double(scratch[1]);
            if (delivery_rate > (6000000.0/8.0)) {
                continue;
            }
            */
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

            // record stall ratios (as a function of scheme and rounded watch time)
            // ignore samples with cubic 
            SchemeStats *the_scheme = nullptr;
            if (scheme == "puffer_ttp_cl/bbr"sv) {
                the_scheme = &puffer;
            } else if (scheme == "mpc/bbr"sv) {
                the_scheme = &mpc;
            } else if (scheme == "robust_mpc/bbr"sv) {
                the_scheme = &robust_mpc;
            } else if (scheme == "pensieve/bbr"sv) {
                the_scheme = &pensieve;
            } else if (scheme == "linear_bba/bbr"sv) {
                the_scheme = &bba;
            }

            if (the_scheme) {
                the_scheme->add_sample(watch_time, stall_time);
                if ( mean_ssim_val >= 0 ) { the_scheme->add_ssim_sample(watch_time, mean_ssim_val); }
                if ( ssim_variation_db_val > 0 and ssim_variation_db_val <= 10000 ) { the_scheme->add_ssim_variation_sample(ssim_variation_db_val); }
            }
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

            // bin corresponding to the simulated watch time is empty => 
            // draw stall ratio from bin to the left
            simulated_watch_time_binned--; 
            size_t num_stall_ratio_samples = scheme.binned_stall_ratios.at(simulated_watch_time_binned).size();
            uniform_int_distribution<> possible_stall_ratio_index(0, num_stall_ratio_samples - 1);
            const double simulated_stall_time = simulated_watch_time * scheme.binned_stall_ratios.at(simulated_watch_time_binned).at(possible_stall_ratio_index(prng)); 

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
        Realizations puffer_r{"Puffer", puffer};
        Realizations mpc_r{"MPC-HM", mpc};
        Realizations robust_mpc_r{"RobustMPC-HM", robust_mpc};
        Realizations pensieve_r{"Pensieve", pensieve};
        Realizations bba_r{"BBA", bba};

        /* For each scheme, take 10000 simulated stall ratios */
        for (unsigned int i = 0; i < iteration_count; i++) {
            if (i % 10 == 0) {
                cerr << "\rsample " << i << "/" << iteration_count << "                    ";
            }

            puffer_r.add_realization(all_watch_times, prng);
            mpc_r.add_realization(all_watch_times, prng);
            robust_mpc_r.add_realization(all_watch_times, prng);
            pensieve_r.add_realization(all_watch_times, prng);
            bba_r.add_realization(all_watch_times, prng);
        }
        cerr << "\n";

        /* report statistics */
        puffer_r.print_samplesize();
        mpc_r.print_samplesize();
        robust_mpc_r.print_samplesize();
        pensieve_r.print_samplesize();
        bba_r.print_samplesize();

        puffer_r.print_summary();
        mpc_r.print_summary();
        robust_mpc_r.print_summary();
        pensieve_r.print_summary();
        bba_r.print_summary();
    }
};

void analyze_main() {
    Statistics stats;

    stats.parse_stdin();
    stats.do_point_estimate();
}

int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 1) {
            cerr << "Usage: " << argv[0] << "\n";
            return EXIT_FAILURE;
        }

        analyze_main();
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
