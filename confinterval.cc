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
#include <google/sparse_hash_map>
#include <google/dense_hash_map>
#include <boost/container_hash/hash.hpp>

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;

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

float to_float(const string_view str) {
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

    const float ret = atof(str.data());

    *null_byte = old_value;

    return ret;
}

class Statistics {
public:
    void parse_stdin() {
	ios::sync_with_stdio(false);
	string line_storage;

	unsigned int line_no = 0;

	vector<string_view> fields, measurement_tag_set_fields, field_key_value;

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
	    if (fields.size() != 3) {
		if (not line.compare(0, 15, "CREATE DATABASE"sv)) {
		    continue;
		}

		cerr << "Ignoring line with wrong number of fields: " << string(line) << "\n";
		continue;
	    }
	}
    }
};

void analyze_main() {
    Statistics stats;

    stats.parse_stdin();
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
