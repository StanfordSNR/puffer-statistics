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

using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;

/** 
 * From stdin, parses influxDB export, which contains one line per key/value datapoint 
 * collected at a given timestamp. Keys correspond to fields in Event, SysInfo, or VideoSent.
 * To stdout, outputs summary of each stream (one stream per line).
 * Takes experimental settings as argument.
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

    optional<uint32_t> init_id{};
    optional<uint32_t> expt_id{};
    optional<uint32_t> user_id{};
    optional<EventType> type{};
    optional<float> buffer{};
    optional<float> cum_rebuf{};

    bool bad = false;

    // Event is "complete" and "good" if all fields are set exactly once
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
                        cerr << "error trying to set contradictory value: ";
                        cerr << *this;   
                    }
                    //		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    /* Set field corresponding to key, if not yet set for this Event.
     * If field is already set with a different value, Event is "bad" */
    void insert_unique(const string_view key, const string_view value, string_table & usernames ) {
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
        
    friend std::ostream& operator<<(std::ostream& out, const Event& s); 
};
std::ostream& operator<< (std::ostream& out, const Event& s) {        
    return out << "init_id=" << s.init_id.value_or(-1)
    << ", expt_id=" << s.expt_id.value_or(-1)
    << ", user_id=" << s.user_id.value_or(-1)
    << ", type=" << (s.type.has_value() ? int(s.type.value()) : 'x')
    << ", buffer=" << s.buffer.value_or(-1.0)
    << ", cum_rebuf=" << s.cum_rebuf.value_or(-1.0)
    << "\n";
}

struct Sysinfo {
    optional<uint32_t> browser_id{};
    optional<uint32_t> expt_id{};
    optional<uint32_t> user_id{};
    optional<uint32_t> init_id{};
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
            and ip == other.ip;
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
        if (key == "init_id"sv) {
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
    << "\n";
}

struct VideoSent {
    optional<float> ssim_index{};
    optional<uint32_t> delivery_rate{}, expt_id{}, init_id{}, user_id{}, size{};

    bool bad = false;

    bool complete() const {
        // ASK: size is optional?
        return ssim_index and delivery_rate and expt_id and init_id and user_id;
    }

    bool operator==(const VideoSent & other) const {
        return ssim_index == other.ssim_index
            and delivery_rate == other.delivery_rate
            and expt_id == other.expt_id
            and init_id == other.init_id
            and user_id == other.user_id
            and size == other.size;
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
                        cerr << "error trying to set contradictory sysinfo value: ";
                        cerr << *this; 
                    }
                    //		throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }
    
    void insert_unique(const string_view key, const string_view value,
            string_table & usernames ) {
        if (key == "init_id"sv) {
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
        << "\n";
}

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

using event_table = map<uint64_t, Event>;
using sysinfo_table = map<uint64_t, Sysinfo>;
using video_sent_table = map<uint64_t, VideoSent>;

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
        
        // sessions[session_key] = vec<[ts, Event]>
        // note channel is part of the key, so "sessions" represents the paper's notion of "streams"
        using session_key = tuple<uint32_t, uint32_t, uint32_t, uint8_t, uint8_t>;
        /*                        init_id,  uid,      expt_id,  server,  channel */
        dense_hash_map<session_key, vector<pair<uint64_t, const Event*>>, boost::hash<session_key>> sessions;

        // sysinfos[sysinfo_key] = SysInfo
        using sysinfo_key = tuple<uint32_t, uint32_t, uint32_t>;
        /*                        init_id,  uid,      expt_id */
        dense_hash_map<sysinfo_key, Sysinfo, boost::hash<sysinfo_key>> sysinfos;

        // chunks[session_key] = vec<[ts, VideoSent]>
        dense_hash_map<session_key, vector<pair<uint64_t, const VideoSent*>>, boost::hash<session_key>> chunks;

        unsigned int bad_count = 0;

        vector<string> experiments{};

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
                experiments.at(experiment_id) = name + "/" + doc["cc"].asString();
            }
        }

    public:
        Parser(const string & experiment_dump_filename)
            : sessions(), sysinfos(), chunks()
        {
            sessions.set_empty_key({0,0,0,-1,-1});
            sysinfos.set_empty_key({0,0,0});
            chunks.set_empty_key({0,0,0,-1,-1});

            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");

            read_experimental_settings_dump(experiment_dump_filename);
        }

        /* Parse lines of influxDB export, for lines measuring client_buffer, client_sysinfo, or video_sent.
         * Each such line contains one field in an Event, SysInfo, or VideoSent (respectively)
         * corresponding to a certain server, channel (for Event/VideoSent only), and timestamp.
         * Store that field in the appropriate Event, SysInfo, or VideoSent (which may already
         * be partially populated by other lines) in client_buffer, client_sysinfo, or video_sent. */
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

                const uint64_t timestamp{to_uint64(timestamp_str)};

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
        }

        /* Group Events by stream (key is {init_id, expt_id, user_id, server, channel}) 
         * Ignore "bad" Events (field was set multiple times), throw for "incomplete" Events (field was never set)
         * Store in sessions, along with timestamp for each Event, ordered by increasing timestamp */
        void accumulate_sessions() {
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                const size_t rss = memcheck() / 1024;
                cerr << "session_server " << int(server) << "/" << client_buffer.size() << ", RSS=" << rss << " MiB\n";
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

                        sessions[{*event.init_id, *event.user_id, *event.expt_id, server, channel}].emplace_back(ts, &event);
                    }
                }
            }
        }

        /* Map each SysInfo to a session (key is {init_id, expt_id, user_id}) 
         * Ignore "bad" SysInfos (field was set multiple times), throw for "incomplete" SysInfos (field was never set)
         * Store in sysinfos; SysInfo is only supplied on load, so there should only be one SysInfo per session */
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
            cerr << "sessions:" << endl;
            for ( const auto & [key, events] : sessions ) {
                cerr << "session key: "; 
                print(key);
                for ( const auto & [ts, event] : events ) {
                    cerr << ts << ", " << *event; 
                }
            }
            cerr << "sysinfos:" << endl;
            for ( const auto & [key, sysinfo] : sysinfos ) {
                cerr << "session key: "; 
                print(key);
                cerr << sysinfo; 
            }
            cerr << "chunks:" << endl;
            for ( const auto & [key, stream_chunks] : chunks ) {
                cerr << "session key: "; 
                print(key);
                for ( const auto & [ts, videosent] : stream_chunks ) {
                    cerr << ts << ", " << *videosent; 
                }
            }
        }

        /* Corresponds to a line of analyze output; summarizes a stream */
        struct EventSummary {
            uint64_t base_time{0};
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
        void analyze_sessions() const {
            float total_time_after_startup=0;
            float total_stall_time=0;
            float total_extent=0;

            unsigned int had_stall=0;
            unsigned int good_sessions=0;
            unsigned int good_and_full=0;

            unsigned int missing_sysinfo = 0;
            unsigned int missing_video_stats = 0;

            size_t overall_chunks = 0;
            size_t overall_high_ssim_chunks = 0;

            for ( auto & [key, events] : sessions ) {
                /* find matching Sysinfo */
                Sysinfo sysinfo{0,0,0,0,0,0};
                int channel_changes = -1;
                for ( unsigned int decrement = 0; decrement < 1024; decrement++ ) {
                    /* Client supplies Sysinfo and init_id on load, then increments init_id
                     * with each channel change. So, decrement init_id until reaching the initial init_id
                     * to find the corresponding Sysinfo */
                    const auto sysinfo_it = sysinfos.find({get<0>(key) - decrement,
                            get<1>(key),
                            get<2>(key)});
                    if (sysinfo_it == sysinfos.end()) {
                        // loop again
                    } else {
                        sysinfo = sysinfo_it->second;
                        channel_changes = decrement;
                        break;
                    }
                }

                if (channel_changes == -1) {
                    missing_sysinfo++;
                }

                const EventSummary summary = summarize(key, events);

                /* find matching videosent stream */
                const auto [normal_ssim_chunks, total_chunks, ssim_sum, mean_delivery_rate, average_bitrate, ssim_variation] = video_summarize(key);
                const double mean_ssim = ssim_sum == -1 ? -1 : ssim_sum / normal_ssim_chunks;
                const size_t high_ssim_chunks = total_chunks - normal_ssim_chunks;

                if (mean_delivery_rate < 0 ) {
                    missing_video_stats++;
                } else {
                    overall_chunks += total_chunks;
                    overall_high_ssim_chunks += high_ssim_chunks;
                }

                cout << fixed;

                // ts from influx export include nanoseconds -- truncate to seconds
                cout << (summary.base_time / 1000000000) << " " << (summary.valid ? "good " : "bad ") << (summary.full_extent ? "full " : "trunc " ) << summary.bad_reason << " "
                    << summary.scheme << " " << inet_ntoa({sysinfo.ip.value()})
                    << " " << ostable.reverse_map(sysinfo.os.value())
                    << " " << channel_changes << " init=" << summary.init_id << " extent=" << summary.time_extent
                    << " used=" << 100 * summary.time_at_last_play / summary.time_extent << "%"
                    << " mean_ssim=" << mean_ssim
                    << " mean_delivery_rate=" << mean_delivery_rate
                    << " average_bitrate=" << average_bitrate
                    << " ssim_variation_db=" << ssim_variation
                    << " startup_delay=" << summary.cum_rebuf_at_startup
                    << " total_after_startup=" << (summary.time_at_last_play - summary.time_at_startup)
                    << " stall_after_startup=" << (summary.cum_rebuf_at_last_play - summary.cum_rebuf_at_startup) 
                    << " total_chunks=" << total_chunks
                    << " high_ssim_chunks=" << high_ssim_chunks << "\n";

                total_extent += summary.time_extent;

                if (summary.valid) {    // valid = "good"
                    good_sessions++;
                    total_time_after_startup += (summary.time_at_last_play - summary.time_at_startup);
                    if (summary.cum_rebuf_at_last_play > summary.cum_rebuf_at_startup) {
                        had_stall++;
                        total_stall_time += (summary.cum_rebuf_at_last_play - summary.cum_rebuf_at_startup);
                    }
                    if (summary.full_extent) {
                        good_and_full++;
                    }
                }
            }

            // mark summary lines with # so confinterval will ignore them
            cout << "#num_sessions=" << sessions.size() << " good=" << good_sessions << " good_and_full=" << good_and_full << " missing_sysinfo=" << missing_sysinfo << " missing_video_stats=" << missing_video_stats << " had_stall=" << had_stall 
                 << " overall_chunks=" << overall_chunks << " overall_high_ssim_chunks=" << overall_high_ssim_chunks << "\n";
            cout << "#total_extent=" << total_extent / 3600.0 << " total_time_after_startup=" << total_time_after_startup / 3600.0 << " total_stall_time=" << total_stall_time / 3600.0 << "\n";
        }

        /* Summarize a list of Videosents, ignoring SSIM ~ 1 */
        // normal_ssim_chunks, total_chunks, ssim_sum, mean_delivery_rate, average_bitrate, ssim_variation]
        tuple<size_t, size_t, double, double, double, double> video_summarize(const session_key & key) const {
            const auto videosent_it = chunks.find(key);
            if (videosent_it == chunks.end()) {
                return { -1, -1, -1, -1, -1, -1 };
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

            for ( const auto [ts, videosent] : chunk_stream ) {
                ssim_cur_db = raw_ssim_to_db(videosent->ssim_index.value());
                if (ssim_cur_db.has_value()) {
                    ssim_sum += videosent->ssim_index.value();
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

            return { num_ssim_samples, chunk_stream.size(), ssim_sum, delivery_rate_sum / chunk_stream.size(), average_bitrate, average_absolute_ssim_variation };
        }

        /* Summarize a list of events corresponding to a stream. */
        EventSummary summarize(const session_key & key, const vector<pair<uint64_t, const Event*>> & events) const {
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

void analyze_main(const string & experiment_dump_filename) {
    Parser parser{ experiment_dump_filename };

    parser.parse_stdin();
    parser.accumulate_sessions();
    parser.accumulate_sysinfos();
    parser.accumulate_video_sents(); 
    parser.analyze_sessions();
}

int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 2) {
            cerr << "Usage: " << argv[0] << " expt_dump [from postgres]\n";
            return EXIT_FAILURE;
        }

        analyze_main(argv[1]);
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
