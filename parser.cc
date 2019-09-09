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
#include <google/sparse_hash_map>

#include <sys/time.h>
#include <sys/resource.h>

using namespace std;
using google::sparse_hash_map;

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

uint64_t to_uint64(const string_view str) {
    uint64_t ret = -1;
    const auto [ptr, ignore] = from_chars(str.data(), str.data() + str.size(), ret);
    if (ptr != str.data() + str.size()) {
	throw runtime_error("could not parse as integer: " + string(str));
    }

    return ret;
}

constexpr uint8_t SERVER_COUNT = 64;

uint64_t get_server_id(const vector<string_view> & fields) {
    uint64_t server_id = -1;
    for (const auto & field : fields) {
	if (not field.compare(0, 10, "server_id=")) {
	    server_id = to_uint64(field.substr(10)) - 1;
	}
    }

    if (server_id >= SERVER_COUNT) {
	for ( const auto & x : fields ) { cerr << "field=" << x << " "; };
	throw runtime_error( "Invalid or missing server id" );
    }

    return server_id;
}

string get_channel(const vector<string_view> & fields) {
    string channel;
    for (const auto & field : fields) {
	if (not field.compare(0, 8, "channel=")) {
	    channel = field.substr(8);
	}
    }

    if (channel.empty()) {
	throw runtime_error("channel missing");
    }

    return channel;
}


class tag_table : public sparse_hash_map<string, string> {
public:
    void insert_unique(string_view key, string_view value) {
	string key_str(key);
	if (find(key_str) != end()) {
	    throw runtime_error("key " + key_str + " already exists");
	}
	insert({move(key_str), string(value)});
    }
};

using key_table = map<uint64_t, tag_table>;

void parse() {
    ios::sync_with_stdio(false);
    string line_storage;
    array<key_table,SERVER_COUNT> client_buffer, video_acked, video_sent;
    key_table client_sysinfo;

    unsigned int line_no = 0;

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

	const vector<string_view> fields = split_on_char(line, ' ');
	if (fields.size() != 3) {
	    if (not line.compare(0, 15, "CREATE DATABASE")) {
		continue;
	    }

	    throw runtime_error("Too many fields in line " + to_string(line_no));
	}

	const auto [measurement_tag_set, field_set, timestamp_str] = tie(fields[0], fields[1], fields[2]);

	const uint64_t timestamp{to_uint64(timestamp_str)};

	const auto measurement_tag_set_fields = split_on_char(measurement_tag_set, ',');
	if (measurement_tag_set_fields.empty()) {
	    throw runtime_error("No measurement field on line " + to_string(line_no));
	}
	const auto measurement = measurement_tag_set_fields[0];

	const auto field_key_value = split_on_char(field_set, '=');
	if (field_key_value.size() != 2) {
	    throw runtime_error("Irregular number of fields in field set: " + string(line));
	}

	const auto [key, value] = tie(field_key_value[0], field_key_value[1]);

	try {
	    if ( measurement == "client_buffer" ) {
		client_buffer[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
	    } else if ( measurement == "active_streams" ) {
		// skip
	    } else if ( measurement == "backlog" ) {
		// skip
	    } else if ( measurement == "channel_status" ) {
		// skip
	    } else if ( measurement == "client_error" ) {
		// skip
	    } else if ( measurement == "client_sysinfo" ) {
		client_sysinfo[timestamp].insert_unique(key, value);
	    } else if ( measurement == "decoder_info" ) {
		// skip
	    } else if ( measurement == "server_info" ) {
		// skip
	    } else if ( measurement == "ssim" ) {
		// skip
	    } else if ( measurement == "video_acked" ) {
		video_acked[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
	    } else if ( measurement == "video_sent" ) {
		video_sent[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
	    } else if ( measurement == "video_size" ) {
		// skip
	    } else {
		throw runtime_error( "Can't parse: " + string(line) );
	    }
	} catch (const exception & e ) {
	    cerr << "Failure on line: " << line << "\n";
	    throw;
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
