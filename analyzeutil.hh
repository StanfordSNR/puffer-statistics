#ifndef ANALYZEUTIL_HH
#define ANALYZEUTIL_HH

#include <stdexcept>
#include <string>
#include <cstdlib>
#include <iostream>
#include <array>
#include <tuple>
#include <charconv>
#include <cstring>
#include <fstream>
#include <google/sparse_hash_map>
#include <google/dense_hash_map>

#include <sys/time.h>
#include <sys/resource.h>
// #include <boost/fusion/adapted/struct.hpp>
// #include <boost/fusion/include/for_each.hpp>
using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;

#define VAR_NAME(var) (#var)

// Uniquely and anonymously identifies a stream
struct public_stream_id {
    // Base64-encoded 32-byte cryptographically secure random ID
    string session_id{};
    
    /* Identifies a stream within a session (unique across streams in a session).
     * Used to group datapoints belonging to the same stream; not particularly
     * meaningful otherwise.
     */
    unsigned index{};
};

size_t memcheck() {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) < 0) {
        perror("getrusage");
        throw std::runtime_error(std::string("getrusage: ") + strerror(errno));
    }

    if (usage.ru_maxrss > 12 * 1024 * 1024) {
        throw std::runtime_error("memory usage is at " + std::to_string(usage.ru_maxrss) + " KiB");
    }

    return usage.ru_maxrss;
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
        throw runtime_error("influx integer " + string(str) + " exceeds max value " + 
                            to_string(numeric_limits<T>::max()));
    }
    return static_cast<T>(ret_64);
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

    /* Return map[key], inserting if necessary. */
    uint32_t forward_map_vivify(const string & key) {
        auto ref = forward_.find(key);
        if (ref == forward_.end()) {	
            forward_[key] = next_id_;
            reverse_[next_id_] = key;
            next_id_++;
            ref = forward_.find(key);
        }
        return ref->second;
    }

    /* Return map.at(key), throwing if not found. */
    uint32_t forward_map(const string & key) const {
        auto ref = forward_.find(key);
        if (ref == forward_.end()) {	
            throw runtime_error( "key " + key + " not found");
        }
        return ref->second;
    }

    const string & reverse_map(const uint32_t id) const {
        auto ref = reverse_.find(id);
        if (ref == reverse_.end()) {
            throw runtime_error( "id " + to_string(id) + " not found");
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

    // Comma-separated anonymous keys and values (to be dumped).
    static string anon_keys() { 
        // In CSV, type is called "event"
        return "event" + string(",") + 
               VAR_NAME(buffer) + "," +
               VAR_NAME(cum_rebuf);
    }
    string anon_values() const { 
        ostringstream values;
        values << string_view(type.value()) << "," 
               << buffer.value() << "," 
               << cum_rebuf.value();
        return values.str();

    }
    // Makes templatizing easier, and enforces that dump() calls the correct function
    // Should only be called on complete datapoints
    string anon_values(const string_table & formats __attribute((unused)) ) const { 
        throw logic_error("Event does not use formats table to retrieve anonymous values");
    }

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
                        cerr << "error trying to set contradictory event value " << value <<
                                " (old value " << field.value() << ")\n";
                        cerr << "Contradictory event with old value:\n";
                        cerr << *this;   
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
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

/* XXX: refactor so field names are easier to update
BOOST_FUSION_ADAPT_STRUCT(Event, (optional<uint32_t>, first_init_id)(optional<uint32_t>, init_id)(optional<uint32_t>, expt_id)(optional<uint32_t>, user_id)(optional<Event::EventType>, type)(optional<float>, buffer)(optional<float>, cum_rebuf)(optional<string>, channel))
*/
    
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
            and first_init_id == other.first_init_id;
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
                        cerr << "error trying to set contradictory sysinfo value " << value <<
                                " (old value " << field.value() << ")\n";
                        cerr << "Contradictory sysinfo:\n";
                        cerr << *this; 
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
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
            // Insert browser to string => id map; store id
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
    optional<float> ssim_index{}, buffer{}, cum_rebuf{};
    optional<uint32_t> delivery_rate{}, expt_id{}, init_id{}, first_init_id{}, user_id{}, size{},
    format{}, cwnd{}, in_flight{}, min_rtt{}, rtt{};
    optional<uint64_t> video_ts{};

    // Comma-separated anonymous keys and values (to be dumped).
    static string anon_keys() { 
        return /* VAR_NAME(video_ts) + string(",") + 
               VAR_NAME(format) + "," +
               VAR_NAME(size) + "," +
               VAR_NAME(ssim_index) + "," +
               VAR_NAME(cwnd) + "," +
               VAR_NAME(in_flight) + "," +
               VAR_NAME(min_rtt) + "," +
               VAR_NAME(rtt) + "," +
               VAR_NAME(delivery_rate) + "," +*/
               VAR_NAME(buffer) + string(",") +
               VAR_NAME(cum_rebuf);
    }
    // Makes templatizing easier, and enforces that dump() calls the correct function
    string anon_values() const { 
        throw logic_error("VideoSent requires formats table to retrieve anonymous values");
    }
    // Should only be called on complete datapoints
    string anon_values(const string_table & formats) const { 
        (void) formats;
        ostringstream values;
        values /* << video_ts.value() << ","
               << formats.reverse_map(format.value()) << ","
               << size.value() << ","
               << ssim_index.value() << ","
               << cwnd.value() << ","
               << in_flight.value() << ","
               << min_rtt.value() << ","
               << rtt.value() << ","
               << delivery_rate.value() << "," */
               << buffer.value() << ","
               << cum_rebuf.value();
        return values.str();
    }

    bool bad = false;

    bool complete() const {
        return ssim_index and delivery_rate and expt_id and init_id and user_id and size and
               video_ts and cwnd and in_flight and min_rtt and rtt and format and
               buffer and cum_rebuf;
    }

    bool operator==(const VideoSent & other) const {
        return ssim_index == other.ssim_index
            and delivery_rate == other.delivery_rate
            and expt_id == other.expt_id
            and init_id == other.init_id
            and user_id == other.user_id
            and size == other.size
            and first_init_id == other.first_init_id
            and video_ts == other.video_ts
            and cwnd == other.cwnd
            and in_flight == other.in_flight
            and min_rtt == other.min_rtt
            and rtt == other.rtt
            and format == other.format
            and buffer == other.buffer
            and cum_rebuf == other.cum_rebuf;
    }

    bool operator!=(const VideoSent & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        cerr << "error trying to set contradictory VideoSent value " << value <<
                                "(old value " << field.value() << ")\n";
                        cerr << "Contradictory VideoSent:\n";
                        cerr << *this; 
                        /* When retroactively adding buffer/cum_rebuf, dump VideoSents with contradictory buf/rebuf values,
                         to avoid changing the number of VideoSents dumped.
                         For such VideoSents, set contradictory field to -1. */
                        if (field == buffer or field == cum_rebuf) {
                            field.emplace(-1.0);
                        } else {
                            bad = true;
                        }
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    void insert_unique(const string_view key, const string_view value,
            string_table & usernames, string_table & formats) {
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
        } else if (key == "video_ts"sv) {
            set_unique( video_ts, influx_integer<uint64_t>( value ) );
        } else if (key == "cwnd"sv) {
            set_unique( cwnd, influx_integer<uint32_t>( value ) );
        } else if (key == "in_flight"sv) {
            set_unique( in_flight, influx_integer<uint32_t>( value ) );
        } else if (key == "min_rtt"sv) {
            set_unique( min_rtt, influx_integer<uint32_t>( value ) );
        } else if (key == "rtt"sv) {
            set_unique( rtt, influx_integer<uint32_t>( value ) );
        } else if (key == "format"sv) {
            set_unique( format, formats.forward_map_vivify(string(value.substr(1,value.size()-2))) );
        } else if (key == "buffer"sv) {
            set_unique( buffer, to_float(value) );
        } else if (key == "cum_rebuffer"sv) {
            set_unique( cum_rebuf, to_float(value) );
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
        << ", video_ts=" << s.video_ts.value_or(-1)
        << ", cwnd=" << s.cwnd.value_or(-1)
        << ", in_flight=" << s.in_flight.value_or(-1)
        << ", min_rtt=" << s.min_rtt.value_or(-1)
        << ", rtt=" << s.rtt.value_or(-1)
        << ", format=" << s.format.value_or(-1)
        << ", buffer=" << s.buffer.value_or(-1)
        << ", cum_rebuf=" << s.cum_rebuf.value_or(-1)
        << "\n";
}

struct VideoAcked {
    optional<uint32_t> expt_id{}, init_id{}, first_init_id{}, user_id{};
    optional<uint64_t> video_ts{};
    optional<float> buffer{}, cum_rebuf{};
   
    // Comma-separated anonymous keys and values (to be dumped)
    static string anon_keys() { 
        return VAR_NAME(video_ts) + string(",") +
               VAR_NAME(buffer) + "," +
               VAR_NAME(cum_rebuf);
    }
    // Should only be called on complete datapoints
    string anon_values() const { 
        ostringstream values;
        values << video_ts.value() << ","
               << buffer.value() << ","
               << cum_rebuf.value();
        return values.str();
    }
    // Makes templatizing easier, and enforces that dump() calls the correct function
    string anon_values(const string_table & formats __attribute((unused)) ) const { 
        throw logic_error("VideoAcked does not use formats table to retrieve anonymous values");
    }

    bool bad = false;

    bool complete() const {
        return expt_id and init_id and user_id and video_ts and buffer and cum_rebuf;
    }

    bool operator==(const VideoAcked & other) const {
        return expt_id == other.expt_id
            and init_id == other.init_id
            and user_id == other.user_id
            and first_init_id == other.first_init_id
            and video_ts == other.video_ts
            and buffer == other.buffer
            and cum_rebuf == other.cum_rebuf;
    }

    bool operator!=(const VideoAcked & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        cerr << "error trying to set contradictory videoacked value " << value <<
                                "(old value " << field.value() << ")\n";
                        cerr << "Contradictory videoacked:\n";
                        cerr << *this; 
                        /* When retroactively adding buffer/cum_rebuf, dump VideoAckeds with contradictory buf/rebuf values,
                         to avoid changing the number of VideoAckeds dumped.
                         For such VideoAckeds, set contradictory field to -1. */
                        if (field == buffer or field == cum_rebuf) {
                            field.emplace(-1.0);
                        } else {
                            bad = true;
                        }
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    void insert_unique(const string_view key, const string_view value,
            string_table & usernames) {
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
        } else if (key == "video_ts"sv) {
            set_unique( video_ts, influx_integer<uint64_t>( value ) );
        } else if (key == "ssim_index"sv) {
            // ignore (already recorded in corresponding video_sent)
        } else if (key == "buffer"sv) {
            set_unique( buffer, to_float(value) );
        } else if (key == "cum_rebuffer"sv) {
            set_unique( cum_rebuf, to_float(value) );
        } else {
            throw runtime_error( "unknown key: " + string(key) );
        }
    }
    friend std::ostream& operator<<(std::ostream& out, const VideoAcked& s); 
};
std::ostream& operator<< (std::ostream& out, const VideoAcked& s) {        
    return out << "init_id=" << s.init_id.value_or(-1)
        << ", expt_id=" << s.expt_id.value_or(-1)
        << ", user_id=" << s.user_id.value_or(-1)
        << ", first_init_id=" << s.first_init_id.value_or(-1)
        << ", video_ts=" << s.video_ts.value_or(-1)
        << ", buffer=" << s.buffer.value_or(-1)
        << ", cum_rebuf=" << s.cum_rebuf.value_or(-1)
        << "\n";
}

struct VideoSize {
    optional<uint64_t> video_ts{};
    optional<uint32_t> size{};
   
    // Comma-separated anonymous keys and values (to be dumped)
    static string anon_keys() { 
        return VAR_NAME(video_ts) + string(",") + 
               VAR_NAME(size);
    }
    // Should only be called on complete datapoints
    string anon_values() const { 
        ostringstream values;
        values << video_ts.value() << ","
               << size.value();
        return values.str();
    }
    // Makes templatizing easier, and enforces that dump() calls the correct function
    string anon_values(const string_table & formats __attribute((unused)) ) const { 
        throw logic_error("VideoSize does not use formats table to retrieve anonymous values");
    }

    bool bad = false;

    bool complete() const {
        return video_ts and size; 
    }

    bool operator==(const VideoSize & other) const {
        return video_ts == other.video_ts 
            and size == other.size;
    }

    bool operator!=(const VideoSize & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        bad = true;
                        cerr << "error trying to set contradictory VideoSent value " << value <<
                                "(old value " << field.value() << ")\n";
                        cerr << "Contradictory VideoSent:\n";
                        cerr << *this; 
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    void insert_unique(const string_view key, const string_view value) {
        /* For video_size and ssim measurements, presentation ts is called "timestamp"
         * (not "video_ts" as in video_sent) */
        if (key == "timestamp"sv) {
            set_unique( video_ts, influx_integer<uint64_t>( value ) );
        } else if (key == "size"sv) {
            set_unique( size, influx_integer<uint32_t>( value ) );
        } else {
            throw runtime_error( "unknown key: " + string(key) );
        }
    }
    friend std::ostream& operator<<(std::ostream& out, const VideoSize& s); 
};
std::ostream& operator<< (std::ostream& out, const VideoSize& s) {        
    return out << "video_ts=" << s.video_ts.value_or(-1)
        << ", size=" << s.size.value_or(-1)
        << "\n";
}

struct SSIM {
    optional<uint64_t> video_ts{};
    optional<float> ssim_index{};
    
    // Comma-separated anonymous keys and values (to be dumped)
    static string anon_keys() { 
        return VAR_NAME(video_ts) + string(",") + 
               VAR_NAME(ssim_index);
    }
    // Should only be called on complete datapoints
    string anon_values() const { 
        ostringstream values;
        values << video_ts.value() << ","
               << ssim_index.value();
        return values.str();
    }
    // Makes templatizing easier, and enforces that dump() calls the correct function
    string anon_values(const string_table & formats __attribute((unused)) ) const { 
        throw logic_error("SSIM does not use formats table to retrieve anonymous values");
    }

    bool bad = false;

    bool complete() const {
        return video_ts and ssim_index; 
    }

    bool operator==(const SSIM & other) const {
        return video_ts == other.video_ts 
            and ssim_index == other.ssim_index;
    }

    bool operator!=(const SSIM & other) const { return not operator==(other); }

    template <typename T>
        void set_unique( optional<T> & field, const T & value ) {
            if (not field.has_value()) {
                field.emplace(value);
            } else {
                if (field.value() != value) {
                    if (not bad) {
                        bad = true;
                        cerr << "error trying to set contradictory SSIM value " << value <<
                                "(old value " << field.value() << ")\n";
                        cerr << "Contradictory SSIM:\n";
                        cerr << *this; 
                    }
                    // throw runtime_error( "contradictory values: " + to_string(field.value()) + " vs. " + to_string(value) );
                }
            }
        }

    void insert_unique(const string_view key, const string_view value) {
        /* For video_size and ssim measurements, presentation ts is called "timestamp"
         * (not "video_ts" as in video_sent) */
        if (key == "timestamp"sv) {
            set_unique( video_ts, influx_integer<uint64_t>( value ) );
        } else if (key == "ssim_index"sv) {
            set_unique( ssim_index, to_float(value) );
        } else {
            throw runtime_error( "unknown key: " + string(key) );
        }
    }
    friend std::ostream& operator<<(std::ostream& out, const SSIM& s); 
};
std::ostream& operator<< (std::ostream& out, const SSIM& s) {        
    return out << "video_ts=" << s.video_ts.value_or(-1)
        << ", ssim_index=" << s.ssim_index.value_or(-1)
        << "\n";
}

// print a tuple of any ssim_index, promoting uint8_t
template<class Tuple, std::size_t N>
struct TuplePrinter {
    static void print(const Tuple& t)
    {
        TuplePrinter<Tuple, N-1>::print(t);
        std::cerr << ", " << +std::get<N-1>(t);
    }
};

template<class Tuple>
struct TuplePrinter<Tuple, 1> {
    static void print(const Tuple& t)
    {
        std::cerr << +std::get<0>(t);
    }
};

template<class... Args>
void print(const std::tuple<Args...>& t)
{
    std::cerr << "(";
    TuplePrinter<decltype(t), sizeof...(Args)>::print(t);
    std::cerr << ")\n";
}

#endif
