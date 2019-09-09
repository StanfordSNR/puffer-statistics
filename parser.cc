#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include <vector>
#include <string>
#include <array>
#include <tuple>
#include <charconv>
#include <map>

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

using key_table = map<uint64_t, vector<string>>;

void parse() {
    ios::sync_with_stdio(false);
    string line_storage;
    array<key_table,SERVER_COUNT> client_buffer;
    key_table active_streams, backlog, channel_status, client_error, client_sysinfo,
	decoder_info, server_info, ssim, video_acked, video_sent, video_size;

    unsigned int line_no = 0;

    while (cin.good()) {
	if (line_no % 1000000 == 0) {
	    cerr << "line " << line_no / 1000000 << "M\n";
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

	if ( measurement == "client_buffer" ) {
	    client_buffer[get_server_id(measurement_tag_set_fields)][timestamp].push_back(string(field_set));
	} else if ( measurement == "active_streams" ) {
	    active_streams[timestamp].push_back(string(field_set));
	} else if ( measurement == "backlog" ) {
	    backlog[timestamp].push_back(string(field_set));
	} else if ( measurement == "channel_status" ) {
	    channel_status[timestamp].push_back(string(field_set));
	} else if ( measurement == "client_error" ) {
	    client_error[timestamp].push_back(string(field_set));
	} else if ( measurement == "client_sysinfo" ) {
	    client_sysinfo[timestamp].push_back(string(field_set));
	} else if ( measurement == "decoder_info" ) {
	    decoder_info[timestamp].push_back(string(field_set));
	} else if ( measurement == "server_info" ) {
	    server_info[timestamp].push_back(string(field_set));
	} else if ( measurement == "ssim" ) {
	    ssim[timestamp].push_back(string(field_set));
	} else if ( measurement == "video_acked" ) {
	    video_acked[timestamp].push_back(string(field_set));
	} else if ( measurement == "video_sent" ) {
	    video_sent[timestamp].push_back(string(field_set));
	} else if ( measurement == "video_size" ) {
	    video_size[timestamp].push_back(string(field_set));
	} else {
	    throw runtime_error( "Can't parse: " + string(line) );
	}
    }

    for (unsigned int server_no = 0; server_no < SERVER_COUNT; server_no++) {
	for (const auto & record : client_buffer[server_no]) {
	    cout << "server=" << server_no << " ts=" << record.first << ":";
	    for (const auto & field : record.second) {
		cout << " " << field;
	    }
	    cout << "\n";
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
