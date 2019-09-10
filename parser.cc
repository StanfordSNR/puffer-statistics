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

template <typename T>
T influx_integer(const string_view str) {
    if (str.back() != 'i') {
	throw runtime_error("invalid influx integer: " + string(str));
    }
    const uint64_t ret_64 = to_uint64(str.substr(0, str.size() - 1));
    if (ret_64 > numeric_limits<T>::max()) {
	throw runtime_error("can't convert to uint32_t: " + string(str));
    }
    return static_cast<T>(ret_64);
}

constexpr uint8_t SERVER_COUNT = 64;

uint64_t get_server_id(const vector<string_view> & fields) {
    uint64_t server_id = -1;
    for (const auto & field : fields) {
	if (not field.compare(0, 10, "server_id="sv)) {
	    server_id = to_uint64(field.substr(10)) - 1;
	}
    }

    if (server_id >= SERVER_COUNT) {
	for ( const auto & x : fields ) { cerr << "field=" << x << " "; };
	throw runtime_error( "Invalid or missing server id" );
    }

    return server_id;
}

class username_table {
    uint32_t next_id_ = 0;

    dense_hash_map<string, uint32_t> forward_{};
    dense_hash_map<uint32_t, string> reverse_{};

public:
    username_table() {
	forward_.set_empty_key({});
	reverse_.set_empty_key(-1);
    }

    uint32_t forward_map_vivify(const string & name) {
	auto ref = forward_.find(name);
	if (ref == forward_.end()) {	
	    forward_[name] = next_id_;
	    reverse_[next_id_] = name;
	    next_id_++;
	    ref = forward_.find(name);
	}
	return ref->second;
    }

    uint32_t forward_map(const string & name) const {
	auto ref = forward_.find(name);
	if (ref == forward_.end()) {	
	    throw runtime_error( "username " + name + " not found");
	}
	return ref->second;
    }

    const string & reverse_map(const uint32_t id) const {
	auto ref = reverse_.find(id);
	if (ref == reverse_.end()) {
	    throw runtime_error( "uid " + to_string(id) + " not found");
	}
	return ref->second;
    }
};

struct Event {
    struct EventType {
	enum class Type : uint8_t { init, startup, play, timer, rebuffer };
	Type type;

	EventType(const string_view sv)
	    : type()
	{
	    if (sv == "timer"sv) { type = Type::timer; }
	    else if (sv == "play"sv) { type = Type::play; }
	    else if (sv == "rebuffer"sv) { type = Type::rebuffer; }
	    else if (sv == "init"sv) { type = Type::init; }
	    else if (sv == "startup"sv) { type = Type::startup; }
	    else { throw runtime_error( "unknown event type: " + string(sv) ); }
	}

	operator uint8_t() const { return static_cast<uint8_t>(type); }

	bool operator==(const EventType other) { return type == other.type; }
	bool operator!=(const EventType other) { return not operator==(other); }
    };

    optional<uint32_t> init_id{};
    optional<uint32_t> expt_id{};
    optional<uint32_t> user_id{};
    optional<EventType> type{};
    optional<float> buffer{};
    optional<float> cum_rebuf{};

    bool bad = false;

    bool complete() const {
	return init_id.has_value() and expt_id.has_value() and user_id.has_value()
	    and type.has_value() and buffer.has_value() and cum_rebuf.has_value();
    }

    template <typename T>
    void set_unique( optional<T> & field, const T & value ) {
	if (not field.has_value()) {
	    field.emplace(value);
	} else {
	    if (field.value() != value) {
		cerr << "error trying to set contradictory value: ";
		cerr << "init_id=" << init_id.value_or(-1)
		     << ", expt_id=" << expt_id.value_or(-1)
		     << ", user_id=" << user_id.value_or(-1)
		     << ", type=" << (type.has_value() ? char(uint8_t(type.value())) : 'x')
		     << ", buffer=" << buffer.value_or(-1.0)
		     << ", cum_rebuf=" << cum_rebuf.value_or(-1.0)
		     << "\n";
		bad = true;
		//		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
	    }
	}
    }

    void insert_unique(const string_view key, const string_view value, username_table & usernames ) {
	if (key == "init_id"sv) {
	    set_unique( init_id, influx_integer<uint32_t>( value ) );
	} else if (key == "expt_id"sv) {
	    set_unique( expt_id, influx_integer<uint32_t>( value ) );
	} else if (key == "user"sv) {
	    if (value.size() <= 2 or value.front() != '"' or value.back() != '"') {
		throw runtime_error("invalid username string: " + string(value));
	    }
	    set_unique( user_id, usernames.forward_map_vivify(string(value.substr(1,value.size()-2))) );
	} else if (key == "event"sv) {
	    set_unique( type, { value.substr(1,value.size()-2) } );
	} else if (key == "buffer"sv) {
	    set_unique( buffer, to_float(value) );
	} else if (key == "cum_rebuf"sv) {
	    set_unique( cum_rebuf, to_float(value) );
	} else {
	    throw runtime_error( "unknown key: " + string(key) );
	}
    }
};

using key_table = map<uint64_t, Event>;

struct Channel {
    constexpr static uint8_t COUNT = 6;

    enum class ID : uint8_t { cbs, nbc, abc, fox, univision, pbs };

    constexpr static array<string_view, COUNT> names = { "cbs", "nbc", "abc", "fox", "univision", "pbs" };

    ID id;

    constexpr Channel(const string_view sv)
	: id()
    {
	if (sv == "cbs"sv) { id = ID::cbs; }
	else if (sv == "nbc"sv) { id = ID::nbc; }
	else if (sv == "abc"sv) { id = ID::abc; }
	else if (sv == "fox"sv) { id = ID::fox; }
	else if (sv == "univision"sv) { id = ID::univision; }
	else if (sv == "pbs"sv) { id = ID::pbs; }
	else { throw runtime_error( "unknown channel: " + string(sv) ); }
    }

    constexpr Channel(const uint8_t id_int) : id(static_cast<ID>(id_int)) {}

    operator string_view() const { return names[uint8_t(id)]; }
    constexpr operator uint8_t() const { return static_cast<uint8_t>(id); }

    bool operator==(const Channel other) { return id == other.id; }
    bool operator!=(const Channel other) { return not operator==(other); }
};

Channel get_channel(const vector<string_view> & fields) {
    for (const auto & field : fields) {
	if (not field.compare(0, 8, "channel="sv)) {
	    return field.substr(8);
	}
    }

    throw runtime_error("channel missing");
}

void parse() {
    ios::sync_with_stdio(false);
    string line_storage;

    username_table usernames;
    array<array<key_table, Channel::COUNT>, SERVER_COUNT> client_buffer;

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

	const auto [measurement_tag_set, field_set, timestamp_str] = tie(fields[0], fields[1], fields[2]);

	const uint64_t timestamp{to_uint64(timestamp_str)};

	split_on_char(measurement_tag_set, ',', measurement_tag_set_fields);
	if (measurement_tag_set_fields.empty()) {
	    throw runtime_error("No measurement field on line " + to_string(line_no));
	}
	const auto measurement = measurement_tag_set_fields[0];

	split_on_char(field_set, '=', field_key_value);
	if (field_key_value.size() != 2) {
	    throw runtime_error("Irregular number of fields in field set: " + string(line));
	}

	const auto [key, value] = tie(field_key_value[0], field_key_value[1]);

	try {
	    if ( measurement == "client_buffer"sv ) {
		client_buffer[get_server_id(measurement_tag_set_fields)][get_channel(measurement_tag_set_fields)][timestamp].insert_unique(key, value, usernames);
	    } else if ( measurement == "active_streams"sv ) {
		// skip
	    } else if ( measurement == "backlog"sv ) {
		// skip
	    } else if ( measurement == "channel_status"sv ) {
		// skip
	    } else if ( measurement == "client_error"sv ) {
		// skip
	    } else if ( measurement == "client_sysinfo"sv ) {
		//		client_sysinfo[timestamp].insert_unique(key, value);
	    } else if ( measurement == "decoder_info"sv ) {
		// skip
	    } else if ( measurement == "server_info"sv ) {
		// skip
	    } else if ( measurement == "ssim"sv ) {
		// skip
	    } else if ( measurement == "video_acked"sv ) {
		//		video_acked[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
	    } else if ( measurement == "video_sent"sv ) {
		//		video_sent[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
	    } else if ( measurement == "video_size"sv ) {
		// skip
	    } else {
		throw runtime_error( "Can't parse: " + string(line) );
	    }
	} catch (const exception & e ) {
	    cerr << "Failure on line: " << line << "\n";
	    throw;
	}
    }

    using session_key = tuple<uint32_t, uint32_t, uint32_t, uint8_t, uint8_t>;
    /*                        init_id,  uid,      expt_id,  server,  channel */
    dense_hash_map<session_key, vector<pair<uint64_t, const Event*>>, boost::hash<session_key>> sessions;
    sessions.set_empty_key({0,0,0,-1,-1});

    unsigned int bad_count = 0;

    for (uint8_t server = 0; server < client_buffer.size(); server++) {
	const size_t rss = memcheck() / 1024;
	cerr << "server " << int(server) << "/" << client_buffer.size() << ", RSS=" << rss << " MiB\n";
	for (uint8_t channel = 0; channel < Channel::COUNT; channel++) {
	    for (const auto & [ts,event] : client_buffer[server][channel]) {
		if (event.bad) {
		    bad_count++;
		    cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
		    continue;
		}
		if (not event.complete()) {
		    throw runtime_error("incomplete event with timestamp " + to_string(ts));
		}

		sessions[{*event.init_id, *event.user_id, *event.expt_id, server, channel}].emplace_back(ts, &event);
	    }
	}
    }

    double total_time = 0;
    double stalled_time = 0;
    unsigned int had_stall = 0;

    for ( const auto & [session_key, events] : sessions ) {
	cout << "Session: " << usernames.reverse_map(get<1>(session_key)) << " lasted " << (events.back().first - events.front().first) / double(1000000000) << " seconds and spent " << events.back().second->cum_rebuf.value() << " seconds stalled\n";
	total_time += (events.back().first - events.front().first) / double(1000000000);
	stalled_time += events.back().second->cum_rebuf.value();
	if (events.back().second->cum_rebuf.value() > 0) {
	    had_stall++;
	}
    }

    cout << "Overall: " << total_time / double(3600) << " hours played, " << 100 * stalled_time / total_time << "% stalled.\n";
    cout << "Out of " << sessions.size() << " sessions, " << had_stall << " had a stall, or " << 100.0 * had_stall / double(sessions.size()) << "%.\n";
    cout << "Memory usage is " << memcheck() / 1024 << " MiB.\n";
    cout << "Bad data points: " << bad_count << "\n";
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
