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

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using namespace std::literals;

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

struct SchemeStats {
    array<vector<double>, 32> binned_stall_ratios{};

    unsigned int samples = 0;
    double total_watch_time = 0;
    double total_stall_time = 0;    

    static unsigned int watch_time_bin(const double raw_watch_time) {
	const int watch_time_bin = lrintf(floorf(log2(raw_watch_time)));
	if (watch_time_bin < 2 or watch_time_bin > 20) {
	    throw runtime_error("binned watch time error");
	}
	return watch_time_bin;
    }

    void add_sample(const double watch_time, const double stall_time) {
	binned_stall_ratios.at(watch_time_bin(watch_time)).push_back(stall_time / watch_time);

	samples++;
	total_watch_time += watch_time;
	total_stall_time += stall_time;	
    }

    double observed_stall_ratio() const {
	return total_stall_time / total_watch_time;
    }
};

class Statistics {
    vector<double> all_watch_times{};

    SchemeStats puffer{}, pufferbetter{};
    SchemeStats mpc{}, robust_mpc{}, pensieve{}, bba{};

public:
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

	    if (line.empty() or line.front() == '#') {
		continue;
	    }

	    if (line.size() > numeric_limits<uint8_t>::max()) {
		throw runtime_error("Line " + to_string(line_no) + " too long");
	    }

	    split_on_char(line, ' ', fields);
	    if (fields.size() != 13) {
		throw runtime_error("Bad line: " + line_storage);
	    }

	    const auto & [ts_str, goodbad, fulltrunc, scheme, ip, os, channelchange, init_id,
			  extent, usedpct, startup_delay, time_after_startup,
			  time_stalled]
		= tie(fields[0], fields[1], fields[2], fields[3],
		      fields[4], fields[5], fields[6], fields[7],
		      fields[8], fields[9], fields[10], fields[11],
		      fields[12]);

	    const uint64_t ts = to_uint64(ts_str);

	    if ((ts >= 1547884800 and ts < 1565193009) or (ts > 1567206883)) {
		// analyze this period: January 19-August 7, and August 30 onward
		// this is the primary study period
	    } else {
		continue;
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

	    // record stall ratios (as a function of scheme and rounded watch time)
	    if (scheme == "puffer_ttp_cl/bbr"sv) {
		puffer.add_sample(watch_time, stall_time);
		pufferbetter.add_sample(watch_time, stall_time * .9);
	    } else if (scheme == "mpc/bbr"sv) {
		mpc.add_sample(watch_time, stall_time);
	    } else if (scheme == "robust_mpc/bbr"sv) {
		robust_mpc.add_sample(watch_time, stall_time);
	    } else if (scheme == "pensieve/bbr"sv) {
		pensieve.add_sample(watch_time, stall_time);
	    } else if (scheme == "linear_bba/bbr"sv) {
		bba.add_sample(watch_time, stall_time);
	    }
	}
    }

    pair<double, double> simulate( default_random_engine & prng, const SchemeStats & scheme ) {
	/* step 1: draw a random watch duration */
	uniform_int_distribution<> possible_watch_time_index(0, all_watch_times.size() - 1);
	const double simulated_watch_time = all_watch_times.at(possible_watch_time_index(prng));
	    
	/* step 2: draw a stall ratio for the scheme from a similar observed watch time */
	unsigned int simulated_watch_time_binned = SchemeStats::watch_time_bin(simulated_watch_time);
	size_t num_stall_ratio_samples = scheme.binned_stall_ratios.at(simulated_watch_time_binned).size();

	uniform_int_distribution<> possible_stall_ratio_index(0, num_stall_ratio_samples - 1);
	const double simulated_stall_time = simulated_watch_time * scheme.binned_stall_ratios.at(simulated_watch_time_binned).at(possible_stall_ratio_index(prng));

	return {simulated_watch_time, simulated_stall_time};
    }

    void do_point_estimate() {
	random_device rd;
	default_random_engine prng(rd());

	constexpr unsigned int iteration_count = 1000;

	vector<double> puffer_stall_ratios;
	vector<double> pufferbetter_stall_ratios;
	vector<double> mpc_stall_ratios;
	vector<double> robust_mpc_stall_ratios;
	vector<double> pensieve_stall_ratios;
	vector<double> bba_stall_ratios;

	for (unsigned int i = 0; i < iteration_count; i++) {
	    if (i % 10 == 0) {
		cerr << "sample " << i << "\n";
	    }

	    SchemeStats puffer_simulated{}, pufferbetter_simulated{};

	    SchemeStats mpc_simulated{}, robust_mpc_simulated{}, pensieve_simulated{}, bba_simulated{};

	    for (unsigned int i = 0; i < puffer.samples; i++) {
		/* simulate a viewer watching for some amount (drawn from the global distribution),
		   and then seeing a stall ratio (drawn from the scheme's distribution of stall ratios
		   from nearby watch times) */

		const auto [puffer_watch, puffer_stall] = simulate(prng, puffer);
		puffer_simulated.add_sample(puffer_watch, puffer_stall);
	    }
	    puffer_stall_ratios.push_back(puffer_simulated.observed_stall_ratio());

	    for (unsigned int i = 0; i < pufferbetter.samples; i++) {
		const auto [pufferbetter_watch, pufferbetter_stall] = simulate(prng, pufferbetter);
		pufferbetter_simulated.add_sample(pufferbetter_watch, pufferbetter_stall);
	    }
	    pufferbetter_stall_ratios.push_back(pufferbetter_simulated.observed_stall_ratio());

	    for (unsigned int i = 0; i < mpc.samples; i++) {
		const auto [mpc_watch, mpc_stall] = simulate(prng, mpc);
		mpc_simulated.add_sample(mpc_watch, mpc_stall);
	    }
	    mpc_stall_ratios.push_back(mpc_simulated.observed_stall_ratio());

	    for (unsigned int i = 0; i < robust_mpc.samples; i++) {
		const auto [robust_mpc_watch, robust_mpc_stall] = simulate(prng, robust_mpc);
		robust_mpc_simulated.add_sample(robust_mpc_watch, robust_mpc_stall);
	    }
	    robust_mpc_stall_ratios.push_back(robust_mpc_simulated.observed_stall_ratio());

	    for (unsigned int i = 0; i < pensieve.samples; i++) {
		const auto [pensieve_watch, pensieve_stall] = simulate(prng, pensieve);
		pensieve_simulated.add_sample(pensieve_watch, pensieve_stall);
	    }
	    pensieve_stall_ratios.push_back(pensieve_simulated.observed_stall_ratio());

	    for (unsigned int i = 0; i < bba.samples; i++) {
		const auto [bba_watch, bba_stall] = simulate(prng, bba);
		bba_simulated.add_sample(bba_watch, bba_stall);
	    }
	    bba_stall_ratios.push_back(bba_simulated.observed_stall_ratio());
	}

	/* report statistics */
	sort(puffer_stall_ratios.begin(), puffer_stall_ratios.end());
	sort(pufferbetter_stall_ratios.begin(), pufferbetter_stall_ratios.end());
	sort(mpc_stall_ratios.begin(), mpc_stall_ratios.end());
	sort(robust_mpc_stall_ratios.begin(), robust_mpc_stall_ratios.end());
	sort(pensieve_stall_ratios.begin(), pensieve_stall_ratios.end());
	sort(bba_stall_ratios.begin(), bba_stall_ratios.end());

	cout << "Puffer stall ratio (95% CI): " << 100 * puffer_stall_ratios[.025 * puffer_stall_ratios.size()] << "% .. " << 100 *puffer_stall_ratios[.975 * puffer_stall_ratios.size()] << "%, median=" << 100 * puffer_stall_ratios[.5 * puffer_stall_ratios.size()] << "%\n";

	cout << "PufferBetter stall ratio (95% CI): " << 100 * pufferbetter_stall_ratios[.025 * pufferbetter_stall_ratios.size()] << "% .. " << 100 *pufferbetter_stall_ratios[.975 * pufferbetter_stall_ratios.size()] << "%, median=" << 100 * pufferbetter_stall_ratios[.5 * pufferbetter_stall_ratios.size()] << "%\n";

	cout << "MPC-HM stall ratio (95% CI): " << 100 * mpc_stall_ratios[.025 * mpc_stall_ratios.size()] << "% .. " << 100 *mpc_stall_ratios[.975 * mpc_stall_ratios.size()] << "%, median=" << 100 * mpc_stall_ratios[.5 * mpc_stall_ratios.size()] << "%\n";

	cout << "Robust_MPC-HM stall ratio (95% CI): " << 100 * robust_mpc_stall_ratios[.025 * robust_mpc_stall_ratios.size()] << "% .. " << 100 *robust_mpc_stall_ratios[.975 * robust_mpc_stall_ratios.size()] << "%, median=" << 100 * robust_mpc_stall_ratios[.5 * robust_mpc_stall_ratios.size()] << "%\n";

	cout << "Pensieve stall ratio (95% CI): " << 100 * pensieve_stall_ratios[.025 * pensieve_stall_ratios.size()] << "% .. " << 100 *pensieve_stall_ratios[.975 * pensieve_stall_ratios.size()] << "%, median=" << 100 * pensieve_stall_ratios[.5 * pensieve_stall_ratios.size()] << "%\n";

	cout << "BBA stall ratio (95% CI): " << 100 * bba_stall_ratios[.025 * bba_stall_ratios.size()] << "% .. " << 100 *bba_stall_ratios[.975 * bba_stall_ratios.size()] << "%, median=" << 100 * bba_stall_ratios[.5 * bba_stall_ratios.size()] << "%\n";
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
