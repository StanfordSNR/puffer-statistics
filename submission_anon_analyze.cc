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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <jsoncpp/json/json.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <dateutil.hh>

using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;

/** 
 * From stdin, parses influxDB export, which contains one line per key/value datapoint 
 * collected at a given timestamp. Keys correspond to fields in Event, SysInfo, or VideoSent.
 * To stdout, outputs summary of each stream (one stream per line).
 * Takes experimental settings and date as arguments.
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

// if delimiter is at end, adds empty string to ret
void split_on_char(const string_view str, const char ch_to_find, vector<string_view> & ret) {
    ret.clear();

    bool in_double_quoted_string = false;
    unsigned int field_start = 0;   // start of next token
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

constexpr uint8_t SERVER_COUNT = 255;

// server_id identifies a daemon serving a given scheme
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

class string_table {
    uint32_t next_id_ = 0;

    dense_hash_map<string, uint32_t> forward_{};
    dense_hash_map<uint32_t, string> reverse_{};

    public:
    string_table() {
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
        constexpr static array<string_view, 5> names = { "init", "startup", "play", "timer", "rebuffer" };

        Type type;

        operator string_view() const { return names[uint8_t(type)]; }

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

        bool operator==(const EventType other) const { return type == other.type; }
        bool operator==(const EventType::Type other) const { return type == other; }
        bool operator!=(const EventType other) const { return not operator==(other); }
        bool operator!=(const EventType::Type other) const { return not operator==(other); }
    };

    /* After 11/27, all measurements are recorded with both first_init_id (identifies session) 
     * and init_id (identifies stream). Before 11/27, only init_id is recorded. */
    optional<uint32_t> first_init_id{}; // optional
    optional<uint32_t> init_id{};       // mandatory
    optional<uint32_t> expt_id{};
    optional<uint32_t> user_id{};
    optional<EventType> type{};
    optional<float> buffer{};
    optional<float> cum_rebuf{};

    bool bad = false;

    // Event is "complete" and "good" if all mandatory fields are set exactly once
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
                    if (not bad) {
                        bad = true;
                        cerr << "error trying to set contradictory event value: ";
                        cerr << *this;   
                    }
                    //		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    /* Set field corresponding to key, if not yet set for this Event.
     * If field is already set with a different value, Event is "bad" */
    void insert_unique(const string_view key, const string_view value, string_table & usernames ) {
        if (key == "first_init_id"sv) {
            set_unique( first_init_id, influx_integer<uint32_t>( value ) );
        } else if (key == "init_id"sv) {
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
        
    friend std::ostream& operator<<(std::ostream& out, const Event& s); 
};
std::ostream& operator<< (std::ostream& out, const Event& s) {        
    return out << "init_id=" << s.init_id.value_or(-1)
    << ", expt_id=" << s.expt_id.value_or(-1)
    << ", user_id=" << s.user_id.value_or(-1)
    << ", type=" << (s.type.has_value() ? int(s.type.value()) : 'x')
    << ", buffer=" << s.buffer.value_or(-1.0)
    << ", cum_rebuf=" << s.cum_rebuf.value_or(-1.0)
    << ", first_init_id=" << s.first_init_id.value_or(-1)
    << "\n";
}

struct Sysinfo {
    optional<uint32_t> browser_id{};
    optional<uint32_t> expt_id{};
    optional<uint32_t> user_id{};
    optional<uint32_t> first_init_id{}; // optional
    optional<uint32_t> init_id{};       // mandatory
    optional<uint32_t> os{};
    optional<uint32_t> ip{};

    bool bad = false;

    bool complete() const {
        return browser_id and expt_id and user_id and init_id and os and ip;
    }

    bool operator==(const Sysinfo & other) const {
        return browser_id == other.browser_id
            and expt_id == other.expt_id
            and user_id == other.user_id
            and init_id == other.init_id
            and os == other.os
            and ip == other.ip
            and first_init_id == first_init_id;
    }

    bool operator!=(const Sysinfo & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        bad = true;
                        cerr << "error trying to set contradictory sysinfo value: ";
                        cerr << *this; 
                    }
                    //		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    void insert_unique(const string_view key, const string_view value,
            string_table & usernames,
            string_table & browsers,
            string_table & ostable ) {
        if (key == "first_init_id"sv) {
            set_unique( first_init_id, influx_integer<uint32_t>( value ) );
        } else if (key == "init_id"sv) {
            set_unique( init_id, influx_integer<uint32_t>( value ) );
        } else if (key == "expt_id"sv) {
            set_unique( expt_id, influx_integer<uint32_t>( value ) );
        } else if (key == "user"sv) {
            if (value.size() <= 2 or value.front() != '"' or value.back() != '"') {
                throw runtime_error("invalid username string: " + string(value));
            }
            set_unique( user_id, usernames.forward_map_vivify(string(value.substr(1,value.size()-2))) );
        } else if (key == "browser"sv) {
            set_unique( browser_id, browsers.forward_map_vivify(string(value.substr(1,value.size()-2))) );
        } else if (key == "os"sv) {
            string osname(value.substr(1,value.size()-2));
            for (auto & x : osname) {
                if ( x == ' ' ) { x = '_'; }
            }
            set_unique( os, ostable.forward_map_vivify(osname) );
        } else if (key == "ip"sv) {
            set_unique( ip, inet_addr(string(value.substr(1,value.size()-2)).c_str()) );
        } else if (key == "screen_width"sv or key == "screen_height"sv) {
            // ignore
        } else {
            throw runtime_error( "unknown key: " + string(key) );
        }
    }
    friend std::ostream& operator<<(std::ostream& out, const Sysinfo& s); 
};
std::ostream& operator<< (std::ostream& out, const Sysinfo& s) {        
    return out << "init_id=" << s.init_id.value_or(-1)
    << ", expt_id=" << s.expt_id.value_or(-1)
    << ", user_id=" << s.user_id.value_or(-1)
    << ", browser_id=" << (s.browser_id.value_or(-1))
    << ", os=" << s.os.value_or(-1.0)
    << ", ip=" << s.ip.value_or(-1.0)
    << ", first_init_id=" << s.first_init_id.value_or(-1)
    << "\n";
}

struct VideoSent {
    optional<float> ssim_index{};
    optional<uint32_t> delivery_rate{}, expt_id{}, init_id{}, first_init_id{}, user_id{}, size{};

    bool bad = false;

    bool complete() const {
        return ssim_index and delivery_rate and expt_id and init_id and user_id and size;
    }

    bool operator==(const VideoSent & other) const {
        return ssim_index == other.ssim_index
            and delivery_rate == other.delivery_rate
            and expt_id == other.expt_id
            and init_id == other.init_id
            and user_id == other.user_id
            and size == other.size
            and first_init_id == first_init_id;
    }

    bool operator!=(const VideoSent & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        bad = true;
                        cerr << "error trying to set contradictory videosent value: ";
                        cerr << *this; 
                    }
                    //		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }
    
    void insert_unique(const string_view key, const string_view value,
            string_table & usernames ) {
        if (key == "first_init_id"sv) {
            set_unique( first_init_id, influx_integer<uint32_t>( value ) );
        } else if (key == "init_id"sv) {
            set_unique( init_id, influx_integer<uint32_t>( value ) );
        } else if (key == "expt_id"sv) {
            set_unique( expt_id, influx_integer<uint32_t>( value ) );
        } else if (key == "user"sv) {
            if (value.size() <= 2 or value.front() != '"' or value.back() != '"') {
                throw runtime_error("invalid username string: " + string(value));
            }
            set_unique( user_id, usernames.forward_map_vivify(string(value.substr(1,value.size()-2))) );
        } else if (key == "ssim_index"sv) {
            set_unique( ssim_index, to_float(value) );
        } else if (key == "delivery_rate"sv) {
            set_unique( delivery_rate, influx_integer<uint32_t>( value ) );
        } else if (key == "size"sv) {
            set_unique( size, influx_integer<uint32_t>( value ) );
        } else if (key == "buffer"sv or key == "cum_rebuffer"sv
                or key == "cwnd"sv or key == "format"sv or key == "in_flight"sv
                or key == "min_rtt"sv or key == "rtt"sv
                or key == "video_ts"sv) {
            // ignore
        } else {
            throw runtime_error( "unknown key: " + string(key) );
        }
    }
    friend std::ostream& operator<<(std::ostream& out, const VideoSent& s); 
};
std::ostream& operator<< (std::ostream& out, const VideoSent& s) {        
    return out << "init_id=" << s.init_id.value_or(-1)
        << ", expt_id=" << s.expt_id.value_or(-1)
        << ", user_id=" << s.user_id.value_or(-1)
        << ", ssim_index=" << s.ssim_index.value_or(-1)
        << ", delivery_rate=" << s.delivery_rate.value_or(-1)
        << ", size=" << s.size.value_or(-1)
        << ", first_init_id=" << s.first_init_id.value_or(-1)
        << "\n";
}

struct Channel {
    constexpr static uint8_t COUNT = 9;

    enum class ID : uint8_t { cbs, nbc, abc, fox, univision, pbs, cw, ion, mnt };

    constexpr static array<string_view, COUNT> names = { "cbs", "nbc", "abc", "fox", "univision", "pbs", "cw", "ion", "mnt" };

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
        else if (sv == "cw"sv) { id = ID::cw; }
        else if (sv == "ion"sv) { id = ID::ion; }
        else if (sv == "mnt"sv) { id = ID::mnt; }
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

using event_table = map<uint64_t, Event>;
using sysinfo_table = map<uint64_t, Sysinfo>;
using video_sent_table = map<uint64_t, VideoSent>;
/* Whenever a timestamp is used to represent a day, round down to Influx backup hour.
 * Influx records ts as nanoseconds - use nanoseconds up until writing ts to stdout. */
using Day_ns = uint64_t;
/* I only want to type this once. */
#define NS_PER_SEC 1000000000UL

#define MAX_SSIM 0.99999    // max acceptable raw SSIM (exclusive) 
// ignore SSIM ~ 1
optional<double> raw_ssim_to_db(const double raw_ssim) {
    if (raw_ssim > MAX_SSIM) return nullopt; 
    return -10.0 * log10( 1 - raw_ssim );
}

class Parser {
    private:
        string_table usernames{};
        string_table browsers{};
        string_table ostable{};

        // client_buffer[server][channel] = map<ts, Event>
        array<array<event_table, Channel::COUNT>, SERVER_COUNT> client_buffer{};
        
        // client_sysinfo[server] = map<ts, SysInfo>
        array<sysinfo_table, SERVER_COUNT> client_sysinfo{};
        
        // video_sent[server][channel] = map<ts, VideoSent>
        array<array<video_sent_table, Channel::COUNT>, SERVER_COUNT> video_sent{}; 
        
        // streams[stream_key] = vec<[ts, Event]>
        using stream_key = tuple<uint32_t, uint32_t, uint32_t, uint8_t, uint8_t>;
        /*                        init_id,  uid,      expt_id,  server,  channel */
        dense_hash_map<stream_key, vector<pair<uint64_t, const Event*>>, boost::hash<stream_key>> streams;

        // sysinfos[sysinfo_key] = SysInfo
        using sysinfo_key = tuple<uint32_t, uint32_t, uint32_t>;
        /*                        init_id,  uid,      expt_id */
        dense_hash_map<sysinfo_key, Sysinfo, boost::hash<sysinfo_key>> sysinfos;

        // chunks[stream_key] = vec<[ts, VideoSent]>
        dense_hash_map<stream_key, vector<pair<uint64_t, const VideoSent*>>, boost::hash<stream_key>> chunks;

        unsigned int bad_count = 0;

        vector<string> experiments{};

        /* Timestamp range to be analyzed (influx export includes corrupt data outside the requested range).
         * Any ts outside this range are rejected */
        pair<Day_ns, Day_ns> days{};
        size_t n_bad_ts = 0;

        void read_experimental_settings_dump(const string & filename) {
            ifstream experiment_dump{ filename };
            if (not experiment_dump.is_open()) {
                throw runtime_error( "can't open " + filename );
            }

            string line_storage;

            while (true) {
                getline(experiment_dump, line_storage);
                if (not experiment_dump.good()) {
                    break;
                }

                const string_view line{line_storage};

                const size_t separator = line.find_first_of(' ');
                if (separator == line.npos) {
                    throw runtime_error("can't find separator: " + line_storage);
                }
                const uint64_t experiment_id = to_uint64(line.substr(0, separator));
                if (experiment_id > numeric_limits<uint16_t>::max()) {
                    throw runtime_error("invalid expt_id: " + line_storage);
                }
                const string_view rest_of_string = line.substr(separator+1);
                Json::Reader reader;
                Json::Value doc;
                reader.parse(string(rest_of_string), doc);
                experiments.resize(experiment_id + 1);
                string name = doc["abr_name"].asString();
                if (name.empty()) {
                    name = doc["abr"].asString();
                }
                // populate experiments with expt_id => abr_name/cc or abr/cc
                experiments.at(experiment_id) = name + "/" + doc["cc"].asString();
            }
        }

    public:
        Parser(const string & experiment_dump_filename, Day_ns start_ts)
            : streams(), sysinfos(), chunks()
        {
            streams.set_empty_key({0,0,0,-1,-1});
            sysinfos.set_empty_key({0,0,0});
            chunks.set_empty_key({0,0,0,-1,-1});

            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");

            read_experimental_settings_dump(experiment_dump_filename);
            days.first = start_ts;
            days.second = start_ts + 60 * 60 * 24 * NS_PER_SEC;
        }


        /* Parse lines of influxDB export, for lines measuring client_buffer, client_sysinfo, or video_sent.
         * Each such line contains one field in an Event, SysInfo, or VideoSent (respectively)
         * corresponding to a certain server, channel (for Event/VideoSent only), and timestamp.
         * Store that field in the appropriate Event, SysInfo, or VideoSent (which may already
         * be partially populated by other lines) in client_buffer, client_sysinfo, or video_sent. 
         * Ignore data points out of the date range. */
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

                // influxDB export line has 3 space-separated fields
                // e.g. client_buffer,channel=abc,server_id=1 cum_rebuf=2.183 1546379215825000000
                split_on_char(line, ' ', fields);
                if (fields.size() != 3) {
                    if (not line.compare(0, 15, "CREATE DATABASE"sv)) {
                        continue;
                    }

                    cerr << "Ignoring line with wrong number of fields: " << string(line) << "\n";
                    continue;
                }
                const auto [measurement_tag_set, field_set, timestamp_str] = tie(fields[0], fields[1], fields[2]);
                // e.g. ["client_buffer,channel=abc,server_id=1", "cum_rebuf=2.183", "1546379215825000000"]

                // skip out-of-range data points
                const uint64_t timestamp{to_uint64(timestamp_str)};
                if (timestamp < days.first or timestamp > days.second) {
                    n_bad_ts++;
                    continue;
                }

                split_on_char(measurement_tag_set, ',', measurement_tag_set_fields);
                if (measurement_tag_set_fields.empty()) {
                    throw runtime_error("No measurement field on line " + to_string(line_no));
                }
                const auto measurement = measurement_tag_set_fields[0]; // e.g. client_buffer

                split_on_char(field_set, '=', field_key_value);          
                if (field_key_value.size() != 2) {
                    throw runtime_error("Irregular number of fields in field set: " + string(line));
                }

                const auto [key, value] = tie(field_key_value[0], field_key_value[1]);  // e.g. [cum_rebuf, 2.183]

                try {
                    if ( measurement == "client_buffer"sv ) {
                        // Set this line's field (e.g. cum_rebuf) in the Event corresponding to this 
                        // server, channel, and ts 
                        const auto server_id = get_server_id(measurement_tag_set_fields);
                        const auto channel = get_channel(measurement_tag_set_fields);

                        client_buffer[server_id][channel][timestamp].insert_unique(key, value, usernames);
                    } else if ( measurement == "active_streams"sv ) {
                        // skip
                    } else if ( measurement == "backlog"sv ) {
                        // skip
                    } else if ( measurement == "channel_status"sv ) {
                        // skip
                    } else if ( measurement == "client_error"sv ) {
                        // skip
                    } else if ( measurement == "client_sysinfo"sv ) {
                        // some records in 2019-09-08T11_2019-09-09T11 have a crazy server_id and
                        // seemingly the older record structure (with user= as part of the tags)
                        optional<uint64_t> server_id;
                        try {
                            server_id.emplace(get_server_id(measurement_tag_set_fields));
                        } catch (const exception & e) {
                            cerr << "Error with server_id: " << e.what() << "\n";
                        }

                        // Set this line's field (e.g. browser) in the SysInfo corresponding to this 
                        // server and ts
                        if (server_id.has_value()) {
                            client_sysinfo[server_id.value()][timestamp].insert_unique(key, value, usernames, browsers, ostable);
                        }
                    } else if ( measurement == "decoder_info"sv ) {
                        // skip
                    } else if ( measurement == "server_info"sv ) {
                        // skip
                    } else if ( measurement == "ssim"sv ) {
                        // skip
                    } else if ( measurement == "video_acked"sv ) {
                        //		video_acked[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
                    } else if ( measurement == "video_sent"sv ) {
                        // Set this line's field (e.g. ssim_index) in the VideoSent corresponding to this 
                        // server, channel, and ts
                        const auto server_id = get_server_id(measurement_tag_set_fields);
                        const auto channel = get_channel(measurement_tag_set_fields);
                        video_sent[server_id][channel][timestamp].insert_unique(key, value, usernames);
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
            // Count total events
            /*
            size_t n_total_events = 0;
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (uint8_t channel = 0; channel < Channel::COUNT; channel++) {
                    n_total_events += client_buffer[server][channel].size();
                }
            }
            cerr << "n_total_events " << n_total_events << endl;
            */
        }

        /* Group Events by stream (key is {init_id, expt_id, user_id, server, channel}) 
         * Ignore "bad" Events (field was set multiple times), throw for "incomplete" Events (field was never set)
         * Store in streams, along with timestamp for each Event, ordered by increasing timestamp */
        void accumulate_streams() {
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                const size_t rss = memcheck() / 1024;
                cerr << "stream_server " << int(server) << "/" << client_buffer.size() << ", RSS=" << rss << " MiB\n";
                for (uint8_t channel = 0; channel < Channel::COUNT; channel++) {
                    // iterates in increasing ts order
                    for (const auto & [ts,event] : client_buffer[server][channel]) {
                        if (event.bad) {
                            bad_count++;
                            cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                            continue;
                        }
                        if (not event.complete()) {
                            throw runtime_error("incomplete event with timestamp " + to_string(ts));
                        }

                        streams[{*event.init_id, *event.user_id, *event.expt_id, server, channel}].emplace_back(ts, &event);
                    }
                }
            }
            // cerr << "n_total_streams " << sessions.size() << endl;  
        }

        /* Map each SysInfo to a stream or session (in the case of older data, when sysinfo was only supplied on load).
         * Key is {init_id, expt_id, user_id}.
         * Ignore "bad" SysInfos (field was set multiple times), throw for "incomplete" SysInfos (field was never set)
         * Store in sysinfos.
         * Use init_id in the key, not first_init_id, since there may be multiple sysinfos per session. */
        void accumulate_sysinfos() {
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                const size_t rss = memcheck() / 1024;
                cerr << "sysinfo_server " << int(server) << "/" << client_buffer.size() << ", RSS=" << rss << " MiB\n";
                for (const auto & [ts,sysinfo] : client_sysinfo[server]) {
                    if (sysinfo.bad) {
                        bad_count++;
                        cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                        continue;
                    }
                    if (not sysinfo.complete()) {
                        throw runtime_error("incomplete sysinfo with timestamp " + to_string(ts));
                    } 

                    const sysinfo_key key{*sysinfo.init_id, *sysinfo.user_id, *sysinfo.expt_id};
                    const auto it = sysinfos.find(key);
                    if (it == sysinfos.end()) {
                        sysinfos[key] = sysinfo;
                    } else {
                        if (sysinfos[key] != sysinfo) {
                            throw runtime_error("contradictory sysinfo for " + to_string(*sysinfo.init_id));
                        }
                    }
                }
            }
        }

        /* Group VideoSents by stream (key is {init_id, expt_id, user_id, server, channel}) 
         * Ignore "bad" VideoSents (field was set multiple times), throw for "incomplete" VideoSents (field was never set)
         * Store in chunks, along with timestamp for each VideoSent */
        void accumulate_video_sents() {
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                const size_t rss = memcheck() / 1024;
                cerr << "video_sent_server " << int(server) << "/" << video_sent.size() << ", RSS=" << rss << " MiB\n";
                for (uint8_t channel = 0; channel < Channel::COUNT; channel++) {
                    for (const auto & [ts,videosent] : video_sent[server][channel]) {
                        if (videosent.bad) {
                            bad_count++;
                            cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                            continue;
                        }
                        if (not videosent.complete()) {
                            throw runtime_error("incomplete videosent with timestamp " + to_string(ts));
                        }

                        chunks[{*videosent.init_id, *videosent.user_id, *videosent.expt_id, server, channel}].emplace_back(ts, &videosent);
                    }
                }
            }
        }

        // print a tuple of any size, promoting uint8_t
        template<class Tuple, std::size_t N>
        struct TuplePrinter {
            static void print(const Tuple& t)
            {
                TuplePrinter<Tuple, N-1>::print(t);
                std::cout << ", " << +std::get<N-1>(t);
            }
        };

        template<class Tuple>
        struct TuplePrinter<Tuple, 1> {
            static void print(const Tuple& t)
            {
                std::cout << +std::get<0>(t);
            }
        };

        template<class... Args>
        void print(const std::tuple<Args...>& t)
        {
            std::cout << "(";
            TuplePrinter<decltype(t), sizeof...(Args)>::print(t);
            std::cout << ")\n";
        }

        void debug_print_grouped_data() {
            cerr << "streams:" << endl;
            for ( const auto & [key, events] : streams ) {
                cerr << "stream key: "; 
                print(key);
                for ( const auto & [ts, event] : events ) {
                    cerr << ts << ", " << *event; 
                }
            }
            cerr << "sysinfos:" << endl;
            for ( const auto & [key, sysinfo] : sysinfos ) {
                cerr << "sysinfo key: "; 
                print(key);
                cerr << sysinfo; 
            }
            cerr << "chunks:" << endl;
            for ( const auto & [key, stream_chunks] : chunks ) {
                cerr << "stream key: "; 
                print(key);
                for ( const auto & [ts, videosent] : stream_chunks ) {
                    cerr << ts << ", " << *videosent; 
                }
            }
        }

        /* Corresponds to a line of analyze output; summarizes a stream */
        struct EventSummary {
            uint64_t base_time{0};  // lowest ts in stream, in NANOseconds
            bool valid{false};      // good or bad
            bool full_extent{true}; // full or trunc
            float time_extent{0};
            float cum_rebuf_at_startup{0};
            float cum_rebuf_at_last_play{0};
            float time_at_startup{0};
            float time_at_last_play{0};

            string scheme{};
            uint32_t init_id{};
            /* reason for bad OR trunc (bad_reason != "good" does not necessarily imply the stream is bad -- 
             * it may just be trunc) */
            string bad_reason{};    
        };

        /* Output a summary of each stream */
        void analyze_streams() const {
            float total_time_after_startup=0;
            float total_stall_time=0;
            float total_extent=0;

            unsigned int had_stall=0;
            unsigned int good_streams=0;
            unsigned int good_and_full=0;

            unsigned int missing_sysinfo = 0;
            unsigned int missing_video_stats = 0;

            size_t overall_chunks = 0, overall_high_ssim_chunks = 0, overall_ssim_1_chunks = 0;

            for ( auto & [key, events] : streams ) {
                /* Find Sysinfo corresponding to this stream. */
                /* Client increments init_id with each channel change.
                 * Before ~11/27/19: must decrement init_id until reaching the initial init_id
                 * to find the corresponding Sysinfo. Also, sysinfo was only supplied on load.
                 * After 11/27: Each data point is recorded with first_init_id and init_id.
                 * Also, sysinfo is supplied on both load and channel change. */
                auto sysinfo_it = sysinfos.end();
                // int channel_changes = -1;
                // use first event to check if stream uses first_init_id
                optional<uint32_t> first_init_id = events.front().second->first_init_id;
                if (first_init_id) {
                    /* We introduced first_init_id at the same time we started sending client_sysinfo 
                     * for every stream, so if a stream has the first_init_id field in its datapoints, 
                     * then that stream should have its own sysinfo
                     * (so no need to decrement to find the sysinfo) */
                    sysinfo_it = sysinfos.find({get<0>(key),
                            get<1>(key),
                            get<2>(key)});
                    // channel_changes = get<0>(key) - first_init_id.value();
                } else {
                    for ( unsigned int decrement = 0; decrement < 1024; decrement++ ) {
                        sysinfo_it = sysinfos.find({get<0>(key) - decrement,
                                get<1>(key),
                                get<2>(key)});
                        if (sysinfo_it == sysinfos.end()) {
                            // loop again
                        } else {
                            // channel_changes = decrement;
                            break;
                        }
                    }
                }

                Sysinfo sysinfo{};
                sysinfo.os = 0;
                sysinfo.ip = 0;
                if (sysinfo_it == sysinfos.end()) {
                    missing_sysinfo++;
                } else {
                    sysinfo = sysinfo_it->second;
                }

                const EventSummary summary = summarize(key, events);

                /* find matching videosent stream */
                const auto [normal_ssim_chunks, ssim_1_chunks, total_chunks, ssim_sum, mean_delivery_rate, average_bitrate, ssim_variation] = video_summarize(key);
                const double mean_ssim = ssim_sum == -1 ? -1 : ssim_sum / normal_ssim_chunks;
                const size_t high_ssim_chunks = total_chunks - normal_ssim_chunks;

                if (mean_delivery_rate < 0 ) {
                    missing_video_stats++;
                } else {
                    overall_chunks += total_chunks;
                    overall_high_ssim_chunks += high_ssim_chunks;
                    overall_ssim_1_chunks += ssim_1_chunks;
                }

                /* When changing the order/name of these fields, update pre_confinterval and confinterval
                 * accordingly (they throw if field name mismatch). 
                 * When changing the number of fields, update constant in watchtimesutil.hh */
                cout << fixed;

                // ts from influx export include nanoseconds -- truncate to seconds
                // cout all non-private values for comparison against 
                // public version 
                cout << "ts=" << (summary.base_time / 1000000000) 
                     << " valid=" << (summary.valid ? "good" : "bad") 
                     << " full_extent=" << (summary.full_extent ? "full" : "trunc" ) 
                     << " bad_reason=" << summary.bad_reason
                     << " scheme=" << summary.scheme 
                     << " extent=" << summary.time_extent
                     << " used=" << 100 * summary.time_at_last_play / summary.time_extent << "%"
                     << " mean_ssim=" << mean_ssim
                     << " mean_delivery_rate=" << mean_delivery_rate
                     << " average_bitrate=" << average_bitrate
                     << " ssim_variation_db=" << ssim_variation
                     << " startup_delay=" << summary.cum_rebuf_at_startup
                     << " total_after_startup=" << (summary.time_at_last_play - summary.time_at_startup)
                     << " stall_after_startup=" << (summary.cum_rebuf_at_last_play - summary.cum_rebuf_at_startup) 
                     << "\n";

                total_extent += summary.time_extent;

                if (summary.valid) {    // valid = "good"
                    good_streams++;
                    total_time_after_startup += (summary.time_at_last_play - summary.time_at_startup);
                    if (summary.cum_rebuf_at_last_play > summary.cum_rebuf_at_startup) {
                        had_stall++;
                        total_stall_time += (summary.cum_rebuf_at_last_play - summary.cum_rebuf_at_startup);
                    }
                    if (summary.full_extent) {
                        good_and_full++;
                    }
                }
            }   // end for

            // mark summary lines with # so confinterval will ignore them
            cout << "#num_streams=" << streams.size() << " good=" << good_streams << " good_and_full=" << good_and_full << " missing_sysinfo=" << missing_sysinfo << " missing_video_stats=" << missing_video_stats << " had_stall=" << had_stall 
                 << " overall_chunks=" << overall_chunks << " overall_high_ssim_chunks=" << overall_high_ssim_chunks 
                 << " overall_ssim_1_chunks=" << overall_ssim_1_chunks << "\n";
            cout << "#total_extent=" << total_extent / 3600.0 << " total_time_after_startup=" << total_time_after_startup / 3600.0 << " total_stall_time=" << total_stall_time / 3600.0 << "\n";
        }

        /* Summarize a list of Videosents, ignoring SSIM ~ 1 */
        // normal_ssim_chunks, ssim_1_chunks, total_chunks, ssim_sum, mean_delivery_rate, average_bitrate, ssim_variation]
        tuple<size_t, size_t, size_t, double, double, double, double> video_summarize(const stream_key & key) const {
            const auto videosent_it = chunks.find(key);
            if (videosent_it == chunks.end()) {
                return { -1, -1, -1, -1, -1, -1, -1 };
            }

            const vector<pair<uint64_t, const VideoSent *>> & chunk_stream = videosent_it->second;

            double ssim_sum = 0;    // raw index
            double delivery_rate_sum = 0;
            double bytes_sent_sum = 0;
            optional<double> ssim_cur_db{};     // empty if index == 1
            optional<double> ssim_last_db{};    // empty if no previous, or previous had index == 1
            double ssim_absolute_variation_sum = 0;
            size_t num_ssim_samples = chunk_stream.size();
            /* variation is calculated between each consecutive pair of chunks */
            size_t num_ssim_var_samples = chunk_stream.size() - 1;  
            size_t num_ssim_1_chunks = 0;

            for ( const auto [ts, videosent] : chunk_stream ) {
                float raw_ssim = videosent->ssim_index.value(); // would've thrown by this point if not set
                if (raw_ssim == 1.0) {
                    num_ssim_1_chunks++; 
                }
                ssim_cur_db = raw_ssim_to_db(raw_ssim);
                if (ssim_cur_db.has_value()) {
                    ssim_sum += raw_ssim;
                } else {
                    num_ssim_samples--; // for ssim_mean, ignore chunk with SSIM == 1
                }

                if (ssim_cur_db.has_value() && ssim_last_db.has_value()) {  
                    ssim_absolute_variation_sum += abs(ssim_cur_db.value() - ssim_last_db.value());
                } else {
                    num_ssim_var_samples--; // for ssim_var, ignore pair containing chunk with SSIM == 1
                }

                ssim_last_db = ssim_cur_db;

                delivery_rate_sum += videosent->delivery_rate.value();
                bytes_sent_sum += videosent->size.value();
            }

            const double average_bitrate = 8 * bytes_sent_sum / (2.002 * chunk_stream.size());

            double average_absolute_ssim_variation = -1;
            if (num_ssim_var_samples > 0) {
                average_absolute_ssim_variation = ssim_absolute_variation_sum / num_ssim_var_samples;
            }

            return { num_ssim_samples, num_ssim_1_chunks, chunk_stream.size(), ssim_sum, delivery_rate_sum / chunk_stream.size(), average_bitrate, average_absolute_ssim_variation };
        }

        /* Summarize a list of events corresponding to a stream. */
        EventSummary summarize(const stream_key & key, const vector<pair<uint64_t, const Event*>> & events) const {
            const auto & [init_id, uid, expt_id, server, channel] = key;

            EventSummary ret;
            ret.scheme = experiments.at(expt_id);
            ret.init_id = init_id;
            ret.bad_reason = "good";

            const uint64_t base_time = events.front().first;
            ret.base_time = base_time;
            ret.time_extent = (events.back().first - base_time) / double(1000000000);

            bool started = false;
            bool playing = false;

            float last_sample = 0.0;

            optional<float> time_low_buffer_started;
            float last_buffer=0, last_cum_rebuf=0;

            /* Break on the first trunc or slow decoder event in the list (if any)
             * Return early if slow decoder, else set validity based on whether stream is  
             * zeroplayed/never started/negative rebuffer. 
             * Bad_reason != "good" indicates that summary is "bad" or "trunc" 
             * (here "bad" refers to some characteristic of the stream, rather than to 
             * contradictory data points as in an Event) */
            for ( unsigned int i = 0; i < events.size(); i++ ) {
                if (not ret.full_extent) {
                    break;  // trunc, but not necessarily bad
                }

                const auto & [ts, event] = events[i];

                const float relative_time = (ts - base_time) / 1000000000.0;

                if (relative_time - last_sample > 8.0) {
                    ret.bad_reason = "event_interval>8s";
                    ret.full_extent = false;
                    break;  // trunc, but not necessarily bad
                }

                if (event->buffer.value() > 0.3) {
                    time_low_buffer_started.reset();
                } else {
                    if (not time_low_buffer_started.has_value()) {
                        time_low_buffer_started.emplace(relative_time);
                    }
                }

                if (time_low_buffer_started.has_value()) {
                    if (relative_time - time_low_buffer_started.value() > 20) {
                        // very long rebuffer
                        ret.bad_reason = "stall>20s";
                        ret.full_extent = false;
                        break;      // trunc, but not necessarily bad
                    }
                }

                if (event->buffer.value() > 5 and last_buffer > 5) {
                    if (event->cum_rebuf.value() > last_cum_rebuf + 0.15) {
                        // stall with plenty of buffer --> slow decoder?
                        ret.bad_reason = "stall_while_playing";
                        return ret; // BAD
                    }
                }

                switch (event->type.value().type) {
                    case Event::EventType::Type::init:
                        break;
                    case Event::EventType::Type::play:
                        playing = true;
                        ret.time_at_last_play = relative_time;
                        ret.cum_rebuf_at_last_play = event->cum_rebuf.value();
                        break;
                    case Event::EventType::Type::startup:
                        if ( not started ) {
                            ret.time_at_startup = relative_time;
                            ret.cum_rebuf_at_startup = event->cum_rebuf.value();
                            started = true;
                        }

                        playing = true;
                        ret.time_at_last_play = relative_time;
                        ret.cum_rebuf_at_last_play = event->cum_rebuf.value();
                        break;
                    case Event::EventType::Type::timer:
                        if ( playing ) {
                            ret.time_at_last_play = relative_time;
                            ret.cum_rebuf_at_last_play = event->cum_rebuf.value();
                        }
                        break;
                    case Event::EventType::Type::rebuffer:
                        playing = false;
                        break;
                }

                last_sample = relative_time;
                last_buffer = event->buffer.value();
                last_cum_rebuf = event->cum_rebuf.value();
            }   // end for

            // zeroplayed and neverstarted are both counted as "didn't begin playing" in paper
            if (ret.time_at_last_play <= ret.time_at_startup) {
                ret.bad_reason = "zeroplayed";      
                return ret; // BAD
            }

            // counted as contradictory data in paper??
            if (ret.cum_rebuf_at_last_play < ret.cum_rebuf_at_startup) {
                ret.bad_reason = "negative_rebuffer";
                return ret; // BAD
            }

            if (not started) {
                ret.bad_reason = "neverstarted";    
                return ret; // BAD
            }

            // good is set here, so validity="bad" iff return early
            ret.valid = true;

            return ret;
        }
};

void analyze_main(const string & experiment_dump_filename, Day_ns start_ts) {
    Parser parser{ experiment_dump_filename, start_ts };

    parser.parse_stdin();
    parser.accumulate_streams();
    parser.accumulate_sysinfos();
    parser.accumulate_video_sents(); 
    parser.analyze_streams();
}

/* Parse date to Unix timestamp (nanoseconds) at Influx backup hour, 
 * e.g. 2019-11-28T11_2019-11-29T11 => 1574938800000000000 (for 11AM UTC backup) */
optional<Day_ns> parse_date(const string & date) {
    const auto T_pos = date.find('T');
    const string & start_day = date.substr(0, T_pos);

    struct tm day_fields{};
    ostringstream strptime_str;
    strptime_str << start_day << " " << BACKUP_HR << ":00:00";
    if (not strptime(strptime_str.str().c_str(), "%Y-%m-%d %H:%M:%S", &day_fields)) {
        return nullopt;
    }

    // set timezone to UTC for mktime
    char* tz = getenv("TZ");
    setenv("TZ", "UTC", 1);
    tzset();

    Day_ns start_ts = mktime(&day_fields) * NS_PER_SEC;

    tz ? setenv("TZ", tz, 1) : unsetenv("TZ");
    tzset();
    return start_ts;
}

/* Must take date as argument, to filter out extra data from influx export */
int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            cerr << "Usage: " << argv[0] << " expt_dump [from postgres] date [e.g. 2019-07-01T11_2019-07-02T11]\n";
            return EXIT_FAILURE;
        }

        optional<Day_ns> start_ts = parse_date(argv[2]); 
        if (not start_ts) {
            cerr << "Date argument could not be parsed; format as 2019-07-01T11_2019-07-02T11\n";
            return EXIT_FAILURE;
        }
        
        analyze_main(argv[1], start_ts.value());
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
