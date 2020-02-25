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
#include "dateutil.hh"
#include "analyzeutil.hh"

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

string get_channel(const vector<string_view> & fields) {
    for (const auto & field : fields) {
        if (not field.compare(0, 8, "channel="sv)) {
            return string(field.substr(8));
        }
    }

    throw runtime_error("channel missing");
}

/* Two datapoints are considered part of the same event (i.e. same Event struct) iff they share 
 * {timestamp, server, channel}. 
 * Two events with the same ts may come to a given server, so use channel to help
 * disambiguate (see 2019-04-30:2019-05-01). 
 * But, don't index client_buffer by server or channel
 * (doesn't allow iterating over streams by timestamp order within
 * a session -- needed for anonymizing)  */
struct event_key {
    uint64_t    timestamp{};
    uint64_t    server{};
    string      channel{};
    bool operator<(const event_key & o) const { 
        return (tie(timestamp, server, channel) < tie(o.timestamp, o.server, o.channel)); 
    }
};

using event_table = map<event_key, Event>;
using sysinfo_table = map<uint64_t, Sysinfo>;

// TODO: update this to match client_buffer
using table_key = tuple<uint64_t, string>;
using video_sent_table = map<table_key, VideoSent>;

/* Two streams are considered part of the same session (i.e. same session_id) iff they share 
 * {decremented init_id, user_id}.
 * Watch_times script used for submission groups session by decremented init_id and IP --
 * this is the closest to that (using IP would mean that streams missing sysinfo
 * wouldn't be assigned a public session ID). */
struct private_stream_id {
    uint32_t    adjusted_init_id{};   // decremented init_id/first_init_id
    uint32_t    user_id{};
    bool operator<(const private_stream_id & o) const { 
        return (tie(adjusted_init_id, user_id) < tie(o.adjusted_init_id, o.user_id)); 
    }
};

/* Two events are considered part of the same stream (i.e. same public_stream_id) iff they share 
 * {private_stream_id, expt_id, server, channel}.
 * Server_id may change mid-session (see 2019-04-30:2019-05-01) - consider this a new stream
 * to match the submission. */
struct stream_id_disambiguation {
    uint32_t    expt_id{};
    uint64_t    server{};
    string      channel{};
    bool operator==(const stream_id_disambiguation & o) const { 
        return (tie(expt_id, server, channel) == tie(o.expt_id, o.server, o.channel));
    }
};

struct stream_index {
    stream_id_disambiguation disambiguation{};
    unsigned                 index{};  // index in list of streams for a session
};
struct public_stream_ids_list {
    public_session_id session_id{}; // random number 
    /* This vector usually has one element, but if the server_id changes mid-session,
     * will contain an element for each server_id. */
    vector<stream_index> streams{};
};
using stream_ids_table = map<private_stream_id, public_stream_ids_list>;
typedef map<private_stream_id, public_stream_ids_list>::iterator stream_ids_iterator;


/* Whenever a timestamp is used to represent a day, round down to Influx backup hour.
 * Influx records ts as nanoseconds - use nanoseconds when writing ts to csv. */
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

        // client_buffer = map<[ts, server, channel], Event>
        // See event_key comment
        event_table client_buffer{};
        
        // client_sysinfo[server] = map<ts, SysInfo>
        array<sysinfo_table, SERVER_COUNT> client_sysinfo{};
        
        // video_sent[server] = map<[ts, channel], VideoSent>
        array<video_sent_table, SERVER_COUNT> video_sent{}; 

        stream_ids_table stream_ids{};
        
        unsigned int bad_count = 0;
        /* Timestamp range to be analyzed (influx export includes corrupt data outside the requested range).
         * Any ts outside this range are rejected */
        pair<Day_ns, Day_ns> days{};
        size_t n_bad_ts = 0;

        
    public:
        Parser(Day_ns start_ts)
        {
            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");

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

                        /* If two events share a {timestamp, server_id, channel}, 
                         * Event will become contradictory and we'll record it as "bad" later
                         * (occurs during study period, e.g. 2019-07-02). */
                        // TODO: remove print
                        // cerr << "setting " << key << ", ts " << timestamp << endl;
                        client_buffer[{timestamp, server_id, channel}].insert_unique(key, value, usernames);
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
                        // TODO: record for dump
                        //		video_acked[get_server_id(measurement_tag_set_fields)][timestamp].insert_unique(key, value);
                    } else if ( measurement == "video_sent"sv ) {
                        // Set this line's field (e.g. ssim_index) in the VideoSent corresponding to this 
                        // server, channel, and ts
                        // TODO: if this works for client_buffer, use for videoSent too (change type of video_sent,
                        // remove channel from struct)
                        const auto server_id = get_server_id(measurement_tag_set_fields);
                        const auto channel = get_channel(measurement_tag_set_fields);
                        video_sent[server_id][{timestamp, channel}].insert_unique(key, value, usernames);
                        // TODO: record other fields for dump
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

        /* Called when stream is found corresponding to a session that hasn't yet been inserted to the map. */
        void record_new_session(const private_stream_id & private_id,
                                const stream_id_disambiguation & disambiguation) {
            /* Generate session id in-place, record with index = 0 */
            public_stream_ids_list new_stream_ids_list{};
            new_stream_ids_list.streams.emplace_back(stream_index{disambiguation, 0});
            int entropy_ret = getentropy(new_stream_ids_list.session_id.data(), BYTES_OF_ENTROPY);
            if (entropy_ret != 0) {
                throw runtime_error(string("Failed to generate public session ID: ") + strerror(errno));
            }
            for (char & c : new_stream_ids_list.session_id) {
                // TODO: hack to display special csv characters: comma, CR, LF, or double quote (RFC 4180)
                if (c == ',' || c == '\r' || c == '\n' || c == '\"') {
                    c = 'a';
                }
            }
            stream_ids.emplace(make_pair(private_id, new_stream_ids_list));
            // cerr << "new session ID " << new_stream_ids_list.session_id.data() << endl;
        }

        /* Assign cryptographically secure session ID to each stream, and record index of stream in session.
         * Session ID and index are outputted in csv; public analyze uses them as a stream ID */
        void anonymize_stream_ids() {
            // iterates in increasing ts order
            for (const auto & [event_info, event] : client_buffer) {
                if (event.bad) {
                    bad_count++;
                    cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                    continue;
                }
                if (not event.complete()) {
                    throw runtime_error("incomplete event with timestamp " + to_string(event_info.timestamp));
                }

                optional<uint32_t> first_init_id = event.first_init_id;
                // Record with first_init_id, if available
                private_stream_id private_id = 
                    {first_init_id.value_or(*event.init_id), *event.user_id};
                const stream_id_disambiguation disambiguation = 
                    {*event.expt_id, event_info.server, event_info.channel};
                // Check if this stream's {init_id, user_id} has already been recorded (via another Event in the stream)
                stream_ids_iterator found_ambiguous_stream = stream_ids.find(private_id);
                if (found_ambiguous_stream != stream_ids.end()) {
                    /* 
                    if (*event.init_id == 2665309711) {
                         cerr << "already recorded session/stream for event with ts " << to_string(ts) << endl; // TODO: remove
                    } 
                    */
                    // "Stream" is defined by {private_stream_id, disambiguation}
                    vector<stream_index> & found_streams = found_ambiguous_stream->second.streams;
                    auto found_disambiguous_stream = find_if(found_streams.begin(), found_streams.end(),
                         [&disambiguation] (const stream_index& s) { return s.disambiguation == disambiguation; });
                    if (found_disambiguous_stream == found_streams.end()) {
                        // Haven't recorded this {expt_id, server, channel} for this {init_id, user_id} yet => 
                        // increment stream index and record
                        unsigned index = found_streams.back().index + 1;
                        found_streams.emplace_back(stream_index{disambiguation, index});
                        /*
                        cerr << "inserted new disambiguous stream with index " << index 
                             << " ts " << event_info.timestamp << endl;
                        cerr << event << endl;
                        */
                    } else {
                        // TODO: remove
                        /*
                        cerr << "disambiguous stream already recorded for this event" 
                             << " ts " << event_info.timestamp << endl;
                        cerr << event << endl;
                        */
                    }
                    continue; 
                }
                if (first_init_id) { 
                    /* No need to decrement -- if stream has first_init_id, record with that */
                    /* Haven't recorded this session yet => generate session id and add to map */
                    record_new_session(private_id, disambiguation);
                    // if already recorded, done (will calculate index on dump)
                } else { 
                    /* client_buffer *is* ordered by timestamp within server (TODO and therefore within session), 
                     * so can decrement to find stream in stream_ids recorded thus far, since session
                     * will already have been recorded if this is not the first stream in the session */
                    // already searched for exact match -- start with decrement = 1
                    unsigned int decrement;
                    for ( decrement = 1; decrement < 1024; decrement++ ) {
                        found_ambiguous_stream = stream_ids.find({private_id.adjusted_init_id - decrement, private_id.user_id});
                        if (found_ambiguous_stream == stream_ids.end()) {
                            // loop again
                        } else {
                            break;
                        }
                    }

                    if (found_ambiguous_stream == stream_ids.end()) {
                        /* This is the first stream in session -- generate session id and add to map */
                        /*
                        if (*event.init_id == 2665309711) {
                            cerr << "didn't find by decrementing; inserting " << to_string(ts) << endl; // TODO: remove
                        }
                        */
                        /*
                        cerr << "recording first stream in session for event" 
                             << " ts " << event_info.timestamp << endl;
                        cerr << event << endl;
                        */
                        record_new_session(private_id, disambiguation);
                    } else {
                        /* Already recorded this session (but not this stream) via the previous ambiguous stream
                         * in the session => copy previous stream's session id, increment stream index, add to map */
                        public_stream_ids_list& found_public_ids = found_ambiguous_stream->second;
                        public_stream_ids_list new_stream_ids_list{}; 
                        new_stream_ids_list.session_id = found_public_ids.session_id;
                        unsigned index = found_public_ids.streams.back().index + 1;
                        new_stream_ids_list.streams.emplace_back(stream_index{disambiguation, index});
                        stream_ids.emplace(make_pair(private_id, new_stream_ids_list));
                        /*
                        if (*event.init_id == 2665309711) {
                            cerr << "found by decrementing; inserting " << to_string(ts) << endl; // TODO: remove
                            cerr << "init_id of found: " << get<0>(private_id) - decrement << endl;
                        }
                        */
                        /*
                        cerr << "added new stream with index " << index << ", session ID " << found_public_ids.session_id.data()
                             << " ts " << event_info.timestamp << endl;
                             */
                    }
                }    
            }   // end client_buffer loop 
            // TODO: remove
            unsigned n_disambiguous_streams = 0;
            for (const auto & [private_id, public_stream_id_list] : stream_ids) {
                n_disambiguous_streams += public_stream_id_list.streams.size();
            }
            cerr << "n_disambiguous_streams in anonymize(): " << n_disambiguous_streams << endl;
        }

        void check_public_stream_id_uniqueness() {
            /*   TODO: update this to work with disambiguation
            set<tuple<public_session_id, unsigned>> unique_public_stream_ids;
            for (const auto & [private_id, public_stream_id] : stream_ids) {
                bool duplicate = unique_public_stream_ids.count({public_stream_id.session_id, public_stream_id.index});
                if (duplicate) {
                    cerr << "duplicate session id: " << public_stream_id.session_id.data() << endl;
                    cerr << "duplicate index: " << public_stream_id.index << endl;
                    throw runtime_error("public stream IDs are not unique across all streams");
                } 
                unique_public_stream_ids.insert({public_stream_id.session_id, public_stream_id.index});
            }
            */
        }

        void debug_print_stream_ids() {
            /*
            // check channels, session_id, channel changes
            cerr << "Events and their IDs:" << endl;
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (const auto & [ts_and_channel, event] : client_buffer[server]) {
                    const string & channel = get<1>(ts_and_channel);
                    cerr << "\n" << event;
                    optional<uint32_t> first_init_id = event.first_init_id;
                    private_stream_id private_id = {first_init_id.value_or(*event.init_id), 
                        *event.user_id, *event.expt_id, server, channel};
                    stream_ids_iterator found_ambiguous_stream = stream_ids.find(private_id);
                    assert (found_ambiguous_stream != stream_ids.end());   // every stream should have been assigned an ID

                    public_stream_id public_id = found_ambiguous_stream->second;
                    cerr << "session ID " << public_id.session_id.data()
                         << "\nindex (always 0 if has first_init_id) " << public_id.index << endl;
                }
            }
            */
        }

        /* 
         * Write csv, including session ID from stream_ids.
         * For events without first_init_id, stream index was also recorded in stream_ids.
         * For events with first_init_id, stream index is calculated as init_id - first_init_id.
         */
        void dump_anonymized_data(const string & date_str) {
            // CLIENT BUFFER
            // line format:
            // ts session_id index expt_id channel <event fields> 
            const string & client_buffer_filename = "client_buffer_" + date_str + ".csv";
            ofstream client_buffer_file;
            client_buffer_file.open(client_buffer_filename);
            if (not client_buffer_file.is_open()) {
                throw runtime_error( "can't open " + client_buffer_filename);
            }
            client_buffer_file << "time (ns GMT),session_id,index,expt_id,channel,event,buffer,cum_rebuf\n";
            for (const auto & [event_info, event] : client_buffer) {
                // already threw for incomplete events when anonymizing
                // TODO: for videosent/acked/sysinfo, check bad/complete when dumping
                if (event.bad) continue;
                optional<uint32_t> first_init_id = event.first_init_id;
                // Look up anonymous session/stream ID
                private_stream_id private_id = 
                    {first_init_id.value_or(*event.init_id), *event.user_id};
                const stream_id_disambiguation disambiguation = 
                    {*event.expt_id, event_info.server, event_info.channel};
                stream_ids_iterator found_ambiguous_stream = stream_ids.find(private_id);
                if (found_ambiguous_stream == stream_ids.end()) {
                    // This shouldn't happen -- all events should have IDs by now
                    throw runtime_error( "Failed to find anonymized session/stream ID for event with init_id " 
                                          + to_string(*event.init_id) + " (ambiguous stream ID not found" );
                }
                public_stream_ids_list& found_public_ids = found_ambiguous_stream->second;
                vector<stream_index> & found_streams = found_public_ids.streams;
                auto found_disambiguous_stream = find_if(found_streams.begin(), found_streams.end(),
                     [&disambiguation] (const stream_index& s) { return s.disambiguation == disambiguation; });
                if (found_disambiguous_stream == found_streams.end()) {
                    // This shouldn't happen -- all events should have IDs by now
                    throw runtime_error( "Failed to find anonymized session/stream ID for event with init_id " 
                                          + to_string(*event.init_id) + " (disambiguous stream ID not found" );
                }
                int index = found_disambiguous_stream->index;
                if (first_init_id) {
                    // If event has first_init_id, its session only appears once in the map,
                    // recorded with index = 0 => calculate index 
                    index = *event.init_id - *first_init_id;
                }

                client_buffer_file << event_info.timestamp << ",";
                // write session_id as raw bytes
                client_buffer_file.write(found_public_ids.session_id.data(), BYTES_OF_ENTROPY);
                client_buffer_file << "," << index
                                   << "," << *event.expt_id << "," << event_info.channel << "," 
                                   << string_view(*event.type) << "," << *event.buffer << "," 
                                   << *event.cum_rebuf << "\n";
            }

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
            }
        }
        // VIDEO SENT
        // line format:
        // ts session_id index expt_id channel <videosent fields>
        // VIDEO ACKED
        // line format:
        // ts session_id index expt_id channel video_ts
        // TODO: also dump sysinfo

};  // end Parser

void private_analyze_main(const string & date_str, Day_ns start_ts) {
    Parser parser{ start_ts };

    parser.parse_stdin();
    parser.anonymize_stream_ids();
    cerr << date_str << endl;   // TODO: remove
    // TODO: put back
    // parser.check_public_stream_id_uniqueness(); // TODO: remove
    // use date_str to name csv
    parser.dump_anonymized_data(date_str);
}

/* Parse date to Unix timestamp (nanoseconds) at Influx backup hour, 
 * e.g. 2019-11-28T11_2019-11-29T11 => 1574938800000000000 (for 11AM UTC backup) */
optional<Day_ns> parse_date(const string & date) {
    // TODO: this definitely works in the configuration used for paper submission (on 96-core VM),
    // but gives local time, not 11am GMT, in VirtualBox without fast cxxflags. Investigate portability.
    const auto T_pos = date.find('T');
    const string & start_day = date.substr(0, T_pos);

    struct tm day_fields{};
    ostringstream strptime_str;
    strptime_str << start_day << " " << BACKUP_HR << ":00:00";
    if (not strptime(strptime_str.str().c_str(), "%Y-%m-%d %H:%M:%S", &day_fields)) {
        return nullopt;
    }
    Day_ns start_ts = mktime(&day_fields) * NS_PER_SEC;
    return start_ts;
}

/* Must take date as argument, to filter out extra data from influx export */
int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 2) {
            cerr << "Usage: " << argv[0] << " date [e.g. 2019-07-01T11_2019-07-02T11]\n";
            return EXIT_FAILURE;
        }

        optional<Day_ns> start_ts = parse_date(argv[1]); 
        if (not start_ts) {
            cerr << "Date argument could not be parsed; format as 2019-07-01T11_2019-07-02T11\n";
            return EXIT_FAILURE;
        }
        
        private_analyze_main(argv[1], start_ts.value());
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
