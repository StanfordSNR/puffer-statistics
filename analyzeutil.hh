#ifndef ANALYZEUTIL_HH
#define ANALYZEUTIL_HH

#include <stdexcept>
#include <string>

// TODO: remove unnecessary includes, check for other shared stuff b/w private and public 
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
// #include <boost/fusion/adapted/struct.hpp>
// #include <boost/fusion/include/for_each.hpp>
using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;
// TODO: combine this file with other utils?

constexpr static unsigned BYTES_OF_ENTROPY = 32;

// 32-byte cryptographically secure random ID
// Has to be std::array to be in tuple, and must be in tuple for hashmap
// (make struct tho to name fields)
using public_session_id = array<char, BYTES_OF_ENTROPY>;
// ~uniquely and anonymously identifies a stream
struct public_stream_id {
    public_session_id session_id{};
    unsigned channel_changes{};
    bool operator==(const public_stream_id other) const { 
        return session_id == other.session_id and 
            channel_changes == other.channel_changes; 
    }
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
        throw runtime_error("can't convert to uint32_t: " + string(str));
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
    optional<string> channel{};

    bool bad = false;

    // Event is "complete" and "good" if all mandatory fields are set exactly once
    bool complete() const {
        return init_id.has_value() and expt_id.has_value() and user_id.has_value()
            and type.has_value() and buffer.has_value() and cum_rebuf.has_value()
            and channel.has_value();
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
                             "(old value " << field.value() << ")\n";
                        cerr << "Contradictory event:\n";
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
        << ", channel=" << s.channel.value_or("None")
        << "\n";
}

/*
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
    optional<string>   channel{};

    bool bad = false;

    bool complete() const {
        return browser_id and expt_id and user_id and init_id and os and ip and channel;
    }

    bool operator==(const Sysinfo & other) const {
        return browser_id == other.browser_id
            and expt_id == other.expt_id
            and user_id == other.user_id
            and init_id == other.init_id
            and os == other.os
            and ip == other.ip
            and first_init_id == other.first_init_id
            and channel == other.channel;
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
                             "(old value " << field.value() << ")\n";
                        cerr << "Contradictory sysinfo:\n";
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
        << ", channel=" << s.channel.value_or("None")
        << "\n";
}

struct VideoSent {
    optional<float> ssim_index{};
    optional<uint32_t> delivery_rate{}, expt_id{}, init_id{}, first_init_id{}, user_id{}, size{};
    optional<string> channel{};

    bool bad = false;

    bool complete() const {
        return ssim_index and delivery_rate and expt_id and init_id and user_id and size and channel;
    }

    bool operator==(const VideoSent & other) const {
        return ssim_index == other.ssim_index
            and delivery_rate == other.delivery_rate
            and expt_id == other.expt_id
            and init_id == other.init_id
            and user_id == other.user_id
            and size == other.size
            and first_init_id == other.first_init_id
            and channel == other.channel;
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
                        cerr << "error trying to set contradictory videosent value " << value <<
                             "(old value " << field.value() << ")\n";
                        cerr << "Contradictory videosent:\n";
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
        << ", channel=" << s.channel.value_or("None")
        << "\n";
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

#endif

/* TODO
 * update comments
 */
