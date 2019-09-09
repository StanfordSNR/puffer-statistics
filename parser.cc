#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include <vector>
#include <string>
#include <array>

using namespace std;

vector<string_view> split_on_char(const string_view str, const char ch_to_find) {
    vector<string_view> ret;

    bool in_double_quoted_string = false;
    unsigned int field_start = 0;
    for (unsigned int i = 0; i < str.size(); i++) {
	const char ch = str[i];
	if (ch == '"') {
	    in_double_quoted_string = !in_double_quoted_string;
	} else if (in_double_quoted_string) {
	    continue;
	} else if (ch == ch_to_find) {
	    ret.push_back(str.substr(field_start, i - field_start));
	    field_start = i + 1;
	}
    }

    ret.push_back(str.substr(field_start));

    return ret;
}

void parse() {
    std::ios::sync_with_stdio(false);
    string line_storage;

    unsigned int line_no = 0;

    while (cin.good()) {
	getline(cin, line_storage);
	line_no++;

	const string_view line{line_storage};

	if (line.empty()) {
	    continue;
	}

	if (line.front() == '#') {
	    continue;
	}

	const string create = "CREATE DATABASE";

	if (not line.compare(0, 15, create)) {
	    continue;
	}

	if (line.size() > std::numeric_limits<uint8_t>::max()) {
	    throw runtime_error("line too long: " + to_string(line.size()) + " bytes");
	}

	auto fields = split_on_char(line, ' ');

	if (fields.size() != 3) {
	    throw runtime_error("Can't parse line " + to_string(line_no) + ": " + string(line));
	}
    }
}

int main() {
    try {
	parse();
    } catch (const exception & e) {
	cerr << e.what() << "\n";
	return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
