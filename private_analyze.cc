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

#include <sys/time.h>
#include <sys/resource.h>
#include <crypto++/base64.h>

#include "dateutil.hh"
#include "analyzeutil.hh"

using namespace std;
using namespace std::literals;
using google::sparse_hash_map;
using google::dense_hash_map;

/** 
 * From stdin, parses influxDB export, which contains one line per key/value "measurement"
 * (e.g. cum_rebuf) collected with a given timestamp, server, and channel. 
 * For each measurement type in {client_buffer, video_sent, video_acked},
 * outputs a csv containing one line per "datapoint" (i.e. all measurements collected 
 * with a given timestamp, server, and channel).
 * Takes date as argument.
 */

#define NS_PER_SEC 1000000000UL
// Bytes of random data used as public session ID after base64 encoding
static constexpr unsigned BYTES_OF_ENTROPY = 32;

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

constexpr uint64_t SERVER_COUNT = 255;

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

/* Two datapoints are considered part of the same event (i.e. same Event struct) iff they share 
 * {timestamp, server, channel}. 
 * Two events with the same ts may come to a given server, so use channel to help
 * disambiguate (see 2019-04-30:2019-05-01 1556622001697000000). */
using event_table = map<uint64_t, Event>;
/*                      timestamp */
using sysinfo_table = map<uint64_t, Sysinfo>;
/*                        timestamp */
using video_sent_table = map<uint64_t, VideoSent>;
/*                           timestamp */
using video_acked_table = map<uint64_t, VideoAcked>;
/*                            timestamp */

/* Fully identifies a stream, for convenience. */
struct private_stream_key {
    optional<uint32_t>  first_init_id;
    uint32_t            init_id;
    uint32_t            user_id;
    uint32_t            expt_id;
    uint64_t            server;
    uint8_t             channel;
};

/* Identifies a group of streams that are part of the same session. 
 * If init_id is a first_init_id, represents a session; else represents a group of streams
 * with the same un-decremented init_id.
 * Two streams are considered part of the same session (i.e. same session_id) iff they share 
 * {decremented init_id/first_init_id, user_id}.
 * Watch_times script used for submission groups session by decremented init_id and IP --
 * this is the closest to that (using IP would mean that streams missing sysinfo
 * wouldn't be assigned a public session ID). */
struct ambiguous_stream_id {
    uint32_t    init_id{};   // UN-decremented init_id, or first_init_id
    uint32_t    user_id{};
    bool operator<(const ambiguous_stream_id & o) const { 
        return (tie(init_id, user_id) < tie(o.init_id, o.user_id)); 
    }
};

/* Two events are considered part of the same stream (i.e. same public_stream_id) iff they share 
 * {ambiguous_stream_id, expt_id, server, channel}.
 * Server_id may change mid-session (see 2019-04-30:2019-05-01) - consider this a new stream
 * to match the submission. */
struct stream_id_disambiguation {
    uint32_t    expt_id{};
    uint64_t    server{};
    uint8_t     channel{};
    bool operator==(const stream_id_disambiguation & o) const { 
        return (tie(expt_id, server, channel) == tie(o.expt_id, o.server, o.channel));
    }
};

/* Represents a disambiguous stream in a list of streams with the
 * same ambiguous stream id. */
struct stream_index {
    stream_id_disambiguation disambiguation{};
    // index in list of streams for a session (-1 if not meaningful)
    int                      index{};  
};

/* Session ID and list of *disambiguous* streams corresponding to a 
 * given *ambiguous* stream ID. */
struct public_stream_ids_list {
    string session_id{}; // random string
    /* This vector usually has one element, but if the server_id changes mid-session,
     * will contain an element for each server_id. */
    vector<stream_index> streams{};
};

using stream_ids_table = map<ambiguous_stream_id, public_stream_ids_list>;
typedef map<ambiguous_stream_id, public_stream_ids_list>::iterator stream_ids_iterator;

/* Whenever a timestamp is used to represent a day, round down to Influx backup hour.
 * Influx records ts as nanoseconds - use nanoseconds when writing ts to csv. */
using Day_ns = uint64_t;

class Parser {
    private:
        string_table usernames{};
        string_table browsers{};
        string_table ostable{};
        string_table formats{};
        
        /* Assign each channel an index, to allow indexing by channel and
         * avoid hard-coding channels */
        string_table channels{};
        /* Get index corresponding to channel. */
        uint8_t get_channel_id(const vector<string_view> & fields) {
            string channel_str; 
            for (const auto & field : fields) {
                if (not field.compare(0, 8, "channel="sv)) {
                    channel_str = string(field.substr(8));
                }
            }

            if (channel_str.empty()) {
                throw runtime_error("channel missing");
            }
            // Insert new channel if needed
            return channels.forward_map_vivify(channel_str);
        }
        
        // client_buffer[server][channel] = map<ts, Event>
        array<vector<event_table>, SERVER_COUNT> client_buffer{}; 
        
        // client_sysinfo[server] = map<ts, SysInfo>
        array<sysinfo_table, SERVER_COUNT> client_sysinfo{};
        
        // video_sent[server][channel] = map<ts, VideoSent>
        array<vector<video_sent_table>, SERVER_COUNT> video_sent{}; 

        // video_acked[server][channel] = map<ts, VideoAcked>
        array<vector<video_acked_table>, SERVER_COUNT> video_acked{}; 

        stream_ids_table stream_ids{};
        
        unsigned int bad_count = 0;
        /* Timestamp range to be analyzed (influx export includes corrupt data outside the requested range).
         * Any ts outside this range (inclusive) are rejected */
        pair<Day_ns, Day_ns> days{};
        size_t n_bad_ts = 0;

        
    public:
        Parser(Day_ns start_ts)
        {
            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");
            formats.forward_map_vivify("unknown");
            channels.forward_map_vivify("unknown");

            days.first = start_ts;
            days.second = start_ts + 60 * 60 * 24 * NS_PER_SEC;
        }

        // Get server and channel ID for a measurement, resizing vector if needed to insert
        // the channel. 
        template<typename Array>
        pair<uint64_t, uint8_t> get_server_channel(Array & arr, vector<string_view> & measurement_tag_set_fields) {
            const uint64_t server_id = get_server_id(measurement_tag_set_fields);
            const uint8_t channel_id = get_channel_id(measurement_tag_set_fields);

            const unsigned n_cur_channels = arr[server_id].size();
            // Insert channel to vector if needed
            int n_new_channels = channel_id + 1 - n_cur_channels;
            if (n_new_channels > 0) {
                 arr[server_id].resize(n_cur_channels + n_new_channels);
            }
            return {server_id, channel_id};
        }

        /* Parse lines of influxDB export, for lines measuring 
         * client_buffer, client_sysinfo, video_sent, or video_acked.
         * Each such line contains one field in an Event, SysInfo, VideoSent, or VideoAcked (respectively)
         * corresponding to a certain server, channel (unless SysInfo), and timestamp.
         * Store that field in the appropriate Event, SysInfo, VideoSent, or VideoAcked (which may already
         * be partially populated by other lines) in client_buffer, client_sysinfo, video_sent, or video_acked
         * data structures. 
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
                        /* Set this line's field in the Event/VideoSent/VideoAcked corresponding to this 
                         * server, channel, and ts. 
                         * If two events share a {timestamp, server_id, channel}, 
                         * Event will become contradictory and we'll record it as "bad" later
                         * (bad events do occur during study period, e.g. 2019-07-02) */
                        const auto & [server_id, channel_id] = 
                            get_server_channel(client_buffer, measurement_tag_set_fields);
                        client_buffer[server_id].at(channel_id)[timestamp].insert_unique(key, value, usernames);
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
                        const auto & [server_id, channel_id] = 
                            get_server_channel(video_acked, measurement_tag_set_fields);
                        video_acked[server_id].at(channel_id)[timestamp].insert_unique(key, value, usernames);
                    } else if ( measurement == "video_sent"sv ) {
                        const auto & [server_id, channel_id] = 
                            get_server_channel(video_sent, measurement_tag_set_fields);
                        video_sent[server_id].at(channel_id)[timestamp].insert_unique(key, value, usernames, formats);
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

        /* Generate random base64-encoded session ID, insert to stream_ids. */
        void generate_session_id(public_stream_ids_list & public_ids_list) {
            uint8_t random_bytes[BYTES_OF_ENTROPY]; // avoid conflicting byte definitions
            int entropy_ret = getentropy(random_bytes, BYTES_OF_ENTROPY);
            if (entropy_ret != 0) {
                throw runtime_error(string("Failed to generate public session ID: ") + strerror(errno));
            }
            // encode as base64 in-place in stream_ids 
            CryptoPP::StringSource ss(random_bytes, BYTES_OF_ENTROPY, true,
                new CryptoPP::Base64Encoder(
                    new CryptoPP::StringSink(public_ids_list.session_id),
                    /* insertLineBreaks = */ false
                ) 
            ); 
        }

        /* Group client_buffer by user_id and first_init_id if available, 
         * else un-decremented init_id.
         * After grouping, each key in stream_ids represents 
         * an ambiguous stream if first_init_id is available, else a session.
         * Record with blank session ID/stream index; will be filled in after grouping. */
        void group_stream_ids() {
            unsigned line_no = 0;
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (uint8_t channel = 0; channel < client_buffer[server].size(); channel++) {
                    for (const auto & [ts,event] : client_buffer[server][channel]) {
                        if (line_no % 1000000 == 0) {
                            const size_t rss = memcheck() / 1024;
                            cerr << "line " << line_no / 1000000 << "M, RSS=" << rss << " MiB\n"; 
                        }
                        line_no++;

                        if (event.bad) {
                            bad_count++;
                            cerr << "Skipping bad data point (of " << bad_count 
                                 << " total) with contradictory values.\n";
                            continue;
                        }
                        if (not event.complete()) {
                            cerr << event << endl;
                            throw runtime_error("incomplete event with timestamp " 
                                                + to_string(ts));
                        }

                        optional<uint32_t> first_init_id = event.first_init_id;
                        // Record with first_init_id, if available
                        ambiguous_stream_id private_id = 
                            {first_init_id.value_or(*event.init_id), *event.user_id};
                        stream_id_disambiguation disambiguation = 
                            {*event.expt_id, server, channel};
                        stream_ids_iterator found_ambiguous_stream = stream_ids.find(private_id);
                        if (found_ambiguous_stream != stream_ids.end() and not first_init_id) {
                            // This stream's {init_id, user_id} has already been recorded => 
                            // add {expt_id, server, channel} if not yet recorded 
                            // (if stream has first_init_id, leave vector empty)
                            vector<stream_index> & found_streams = found_ambiguous_stream->second.streams;
                            auto found_disambiguous_stream = find_if(found_streams.begin(), found_streams.end(),
                                 [&disambiguation] (const stream_index& s) { 
                                    return s.disambiguation == disambiguation; 
                            });

                            if (found_disambiguous_stream == found_streams.end()) {
                                // Haven't recorded this {expt_id, server, channel} for this {init_id, user_id} yet =>
                                // add an entry for this {expt_id, server, channel} (regardless of first_init_id)
                                found_streams.emplace_back(stream_index{disambiguation, -1});
                            } else {
                                // Already recorded this {expt_id, server, channel} for this {init_id, user_id} =>
                                // nothing to do
                            }
                        } else if (found_ambiguous_stream == stream_ids.end()) {
                            // This stream's {init_id, user_id} has not yet been recorded => 
                            // add an entry in stream_ids
                            // with blank session ID and stream indexes (will be filled in in pass over stream_ids) 
                            // (regardless of first_init_id)
                            /* Generate session id in-place, record with index = -1 */
                            public_stream_ids_list new_stream_ids_list{};
                            if (not first_init_id) {
                                new_stream_ids_list.streams.emplace_back(stream_index{disambiguation, -1});
                            }
                            stream_ids.emplace(make_pair(private_id, new_stream_ids_list));
                        }
                    }
                }
            }
        }

        /* Assign cryptographically secure session ID to each stream, and record index of stream in session.
         * Session ID and index are outputted in csv; public analyze uses them as a stream ID. 
         * Build stream_ids using client_buffer events. 
         * When dumping data, search stream_ids for stream key corresponding to each datapoint */
        void anonymize_stream_ids() {
            /* Pass over stream IDs; all init_ids are recorded, so it's safe to decrement to search
             * for previous streams in the session.
             * This pass is required because sometimes two events have the same timestamp and user, but
             * the second one has a lower init_id (e.g. see 2019-03-31, init_ids 3977886231/3977886232)
             * So, there's no obvious way to build client_buffer such that init_ids within a session 
             * are sorted -- sorting by ts, server, and channel is not enough. */
            for (auto & [cur_private_id, cur_public_ids_list] : stream_ids) {
                bool is_first_init_id = cur_public_ids_list.streams.empty();
                if (is_first_init_id) { 
                    /* Streams with first_init_id are recorded with empty list of disambiguous
                     * streams, since there's no need to calculate a stream index (can be 
                     * calculated with subtraction on dump) */
                    /* No need to decrement -- if stream has first_init_id, record with that */
                    generate_session_id(cur_public_ids_list);
                } else { 
                    stream_ids_iterator found_ambiguous_stream;
                    // Searching for a *different* session than the current one => start with decrement = 1
                    unsigned decrement;
                    for ( decrement = 1; decrement < 1024; decrement++ ) {
                        found_ambiguous_stream = stream_ids.find({
                                cur_private_id.init_id - decrement, cur_private_id.user_id});
                        if (found_ambiguous_stream == stream_ids.end()) {
                            // loop again
                        } else {
                            break;
                        }
                    }

                    unsigned start_stream_index = 0;

                    if (found_ambiguous_stream == stream_ids.end()) {
                        /* This is the first stream in session -- generate session id,
                         * fill in stream indexes starting from 0. */
                        generate_session_id(cur_public_ids_list);
                    } else {
                        /* Already recorded this session via the previous ambiguous stream
                         * in the session => copy previous stream's session id, 
                         * fill in stream indexes starting from previous stream's last one */
                        public_stream_ids_list& found_public_ids = found_ambiguous_stream->second;
                        cur_public_ids_list.session_id = found_public_ids.session_id;
                        start_stream_index = found_public_ids.streams.back().index + 1;
                    }
                    // Fill in stream indexes, if no first_init_id
                    for (auto & disambiguous_stream : cur_public_ids_list.streams) {
                        disambiguous_stream.index = start_stream_index++;
                    }
                }   // end else 
            }   // end stream_ids loop
        }

        /* Useful for testing. Check that no two streams are assigned the same
         * {session_id, stream index} */
        void check_public_stream_id_uniqueness() const {
            set<tuple<string, unsigned>> unique_public_stream_ids;
            for (const auto & [private_id, public_ids_list] : stream_ids) {
                for (const auto & disambiguous_stream : public_ids_list.streams) {
                    bool duplicate = unique_public_stream_ids.count(
                            {public_ids_list.session_id, disambiguous_stream.index});
                    if (duplicate) {
                        cerr << "duplicate public ID:" << endl;
                        cerr << "duplicate init id: " << private_id.init_id << endl;
                        cerr << "duplicate index: " << disambiguous_stream.index << endl;
                        throw runtime_error("public stream IDs are not unique across all streams");
                    } 
                    unique_public_stream_ids.insert({public_ids_list.session_id, disambiguous_stream.index});
                }
            }
        }

        /* Given private stream id, return public session ID and stream index.
         * Throws if public IDs not found, which represents some logic error for
         * client_buffer, but not for video_sent (e.g. 2019-03-30T11_2019-03-31T11
         * has a video_sent with init_id 901804980 belonging to a stream with no corresponding 
         * client_buffer.)
         * XXX: could still record chunks like these in csv
         * (by using video_sent as well as client_buffer to build stream_ids), 
         * but a video_sent with no corresponding 
         * client_buffer seems spurious, so ignore such chunks for now. */
        public_stream_id get_anonymous_ids(const private_stream_key & stream_key) {
            optional<uint32_t> first_init_id = stream_key.first_init_id;
            // Look up anonymous session/stream ID
            ambiguous_stream_id private_id = 
                {first_init_id.value_or(stream_key.init_id), stream_key.user_id};
            const stream_id_disambiguation disambiguation = 
                {stream_key.expt_id, stream_key.server, stream_key.channel};
            const stream_ids_iterator found_ambiguous_stream = stream_ids.find(private_id);
            if (found_ambiguous_stream == stream_ids.end()) {
                throw runtime_error( "Failed to find anonymized session/stream ID for init_id " 
                                      + to_string(stream_key.init_id) + ", user " + to_string(stream_key.user_id) 
                                      + " (ambiguous stream ID not found)" );
            }
            
            unsigned index;
            public_stream_ids_list& found_public_ids = found_ambiguous_stream->second;
           
            if (first_init_id) {
                // If private_stream_key has first_init_id, its session only appears once in stream_ids,
                // calculate index 
                index = stream_key.init_id - *first_init_id;
            } else {
                vector<stream_index> & found_streams = found_public_ids.streams;
                auto found_disambiguous_stream = find_if(found_streams.begin(), found_streams.end(),
                     [&disambiguation] (const stream_index& s) { return s.disambiguation == disambiguation; });
                if (found_disambiguous_stream == found_streams.end()) {
                    throw runtime_error( "Failed to find anonymized session/stream ID for init_id " 
                                          + to_string(stream_key.init_id) 
                                          + " (disambiguous stream ID not found)" );
                }
                index = found_disambiguous_stream->index;
            }

            return {found_public_ids.session_id, index};
        }
        /* 
         * Write csv, including session ID from stream_ids.
         * For events without first_init_id, stream index was also recorded in stream_ids.
         * For events with first_init_id, stream index is calculated as init_id - first_init_id.
         */
        void dump_anonymized_client_buffer(const string & date_str) {
            // line format:
            // ts session_id index expt_id channel <event fields> 
            const string & client_buffer_filename = "client_buffer_" + date_str + ".csv";
            ofstream client_buffer_file{client_buffer_filename};
            if (not client_buffer_file.is_open()) {
                throw runtime_error( "can't open " + client_buffer_filename);
            }
            client_buffer_file << "time (ns GMT),session_id,index,expt_id,channel,event,buffer,cum_rebuf\n";
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (uint8_t channel = 0; channel < client_buffer[server].size(); channel++) {
                    for (const auto & [ts,event] : client_buffer[server][channel]) {
                        // already printed bad event info and threw for incomplete events when anonymizing
                        if (event.bad) continue;
                        // Don't catch -- see comment on get_anonymous_ids
                        public_stream_id public_id = get_anonymous_ids({event.first_init_id, *event.init_id,
                                *event.user_id, *event.expt_id, server, channel});
                        client_buffer_file << ts << "," 
                                           << public_id.session_id << ","
                                           << public_id.index << ","
                                           << *event.expt_id << "," 
                                           << channels.reverse_map(channel) << "," 
                                           << string_view(*event.type) << "," 
                                           << *event.buffer << "," 
                                           << *event.cum_rebuf << "\n";
                    }
                }
            }

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
            }
        }

        void dump_anonymized_video_sent(const string & date_str) {
            // line format:
            // ts session_id index expt_id channel <video_sent fields>
            const string & video_sent_filename = "video_sent_" + date_str + ".csv";
            ofstream video_sent_file{video_sent_filename};
            if (not video_sent_file.is_open()) {
                throw runtime_error( "can't open " + video_sent_filename);
            }
            video_sent_file << "time (ns GMT),session_id,index,expt_id,channel,"
                               "video_ts,format,size,ssim_index,cwnd,in_flight,min_rtt,rtt,delivery_rate\n";
            for (uint64_t server = 0; server < video_sent.size(); server++) {
                for (uint8_t channel = 0; channel < video_sent[server].size(); channel++) {
                    for (const auto & [ts,video_sent] : video_sent[server][channel]) {
                        if (video_sent.bad) {
                            bad_count++;
                            cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                            continue;
                        }
                        if (not video_sent.complete()) {
                            cerr << video_sent << endl;
                            throw runtime_error("incomplete video_sent with timestamp " + to_string(ts));
                        }

                        const string & format_str = formats.reverse_map(*video_sent.format);
                        // catch -- see comment on get_anonymous_ids
                        public_stream_id public_id; 
                        try {
                            public_id = 
                                get_anonymous_ids({video_sent.first_init_id, *video_sent.init_id,
                                    *video_sent.user_id, *video_sent.expt_id, server, channel});
                        } catch (const exception & e) {
                            cerr << "Chunk with timestamp " << ts << " has no corresponding event: " 
                                 << e.what() << "\n";
                            continue;   // don't dump this chunk
                        }
                        video_sent_file << ts << ","
                                        << public_id.session_id << ","
                                        << public_id.index << ","
                                        << *video_sent.expt_id << "," << channels.reverse_map(channel) << "," 
                                        << *video_sent.video_ts << "," << format_str << ","
                                        << *video_sent.size << "," << *video_sent.ssim_index << ","
                                        << *video_sent.cwnd << "," << *video_sent.in_flight << ","
                                        << *video_sent.min_rtt << "," << *video_sent.rtt << ","
                                        << *video_sent.delivery_rate << "\n";
                    }
                }
            }

            video_sent_file.close();   
            if (video_sent_file.bad()) {
                throw runtime_error("error writing " + video_sent_filename);
            }
        }
        void dump_anonymized_video_acked(const string & date_str) {
            // line format:
            // ts session_id index expt_id channel <video_acked fields>
            const string & video_acked_filename = "video_acked_" + date_str + ".csv";
            ofstream video_acked_file{video_acked_filename};
            if (not video_acked_file.is_open()) {
                throw runtime_error( "can't open " + video_acked_filename);
            }
            video_acked_file << "time (ns GMT),session_id,index,expt_id,channel,video_ts\n";
            for (uint64_t server = 0; server < video_acked.size(); server++) {
                for (uint8_t channel = 0; channel < video_acked[server].size(); channel++) {
                    for (const auto & [ts,video_acked] : video_acked[server][channel]) {
                        if (video_acked.bad) {
                            bad_count++;
                            cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                            continue;
                        }
                        if (not video_acked.complete()) {
                            cerr << video_acked << endl;
                            throw runtime_error("incomplete video_acked with timestamp " + to_string(ts));
                        }

                        public_stream_id public_id; 
                        try {
                            public_id = 
                                get_anonymous_ids({video_acked.first_init_id, *video_acked.init_id,
                                    *video_acked.user_id, *video_acked.expt_id, server, channel});
                        } catch (const exception & e) {
                            cerr << "VideoAcked with timestamp " << ts << " has no corresponding event: " 
                                 << e.what() << "\n";
                            continue;   // don't dump this chunk
                        }
                        video_acked_file << ts << ","
                                         << public_id.session_id << ","
                                         << public_id.index << ","
                                         << *video_acked.expt_id << "," << channels.reverse_map(channel) << "," 
                                         << *video_acked.video_ts << "\n";
                    }
                }
            }

            video_acked_file.close();   
            if (video_acked_file.bad()) {
                throw runtime_error("error writing " + video_acked_filename);
            }
        }
};  // end Parser

void private_analyze_main(const string & date_str, Day_ns start_ts) {
    Parser parser{ start_ts };

    parser.parse_stdin();
    parser.group_stream_ids();
    parser.anonymize_stream_ids(); 
    // parser.check_public_stream_id_uniqueness(); // remove (test only)
    // use date_str to name csv
    parser.dump_anonymized_client_buffer(date_str); 
    parser.dump_anonymized_video_sent(date_str); 
    parser.dump_anonymized_video_acked(date_str); 
    // TODO: also dump sysinfo?
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

        optional<Day_sec> start_ts = str2Day_sec(argv[1]);
        if (not start_ts) {
            cerr << "Date argument could not be parsed; format as 2019-07-01T11_2019-07-02T11\n";
            return EXIT_FAILURE;
        }
        
         // convert start_ts to ns for comparison against Influx ts
        private_analyze_main(argv[1], start_ts.value() * NS_PER_SEC);
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
