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
using video_size_table = map<uint64_t, VideoSize>;
/*                           timestamp */
using ssim_table = map<uint64_t, SSIM>;
/*                               timestamp */

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

    /* Maps field value to int, for string values */
    string_table usernames{};
    string_table browsers{};
    string_table ostable{};
    string_table formats{};
    string_table channels{};
    
    /* Measurement arrays */
    
    // Just used to reserve vectors
    static constexpr unsigned N_CHANNELS_ESTIMATE = 10;
    static constexpr unsigned N_FORMATS_ESTIMATE = 10;
    
    // Note: dump_measurement() uses the names of the measurement array members

    // client_buffer[server][channel] = map<ts, Event>
    array<vector<event_table>, SERVER_COUNT> client_buffer{}; 
    
    // client_sysinfo[server] = map<ts, SysInfo>
    array<sysinfo_table, SERVER_COUNT> client_sysinfo{};
    
    // video_sent[server][channel] = map<ts, VideoSent>
    array<vector<video_sent_table>, SERVER_COUNT> video_sent{}; 

    // video_acked[server][channel] = map<ts, VideoAcked>
    array<vector<video_acked_table>, SERVER_COUNT> video_acked{}; 
    
    // video_size[format][channel] = map<ts, VideoSize>
    // Insert the estimated number of (empty) inner vectors, so they can be reserved up front
    vector<vector<video_size_table>> video_size = vector<vector<video_size_table>>(N_FORMATS_ESTIMATE); 
    
    // ssim[format][channel] = map<ts, SSIM>
    // Insert the estimated number of (empty) inner vectors, so they can be reserved up front
    vector<vector<ssim_table>> ssim = vector<vector<ssim_table>>(N_FORMATS_ESTIMATE); 

    stream_ids_table stream_ids{};
    
    unsigned int bad_count = 0;
    /* Timestamp range to be analyzed (influx export includes partial datapoints outside the requested range).
     * Any ts outside this range (inclusive) are rejected */
    pair<Day_ns, Day_ns> days{};
    size_t n_bad_ts = 0;
    
    /* Date to analyze, e.g. 2019-07-01T11_2019-07-02T11 */
    const string date_str{};
        
    /* Get index corresponding to the string value of a tag. 
     * Updates the tag's string <=> index table as needed.
     * Useful for a field whose value is used to index into a measurement array
     * (e.g. format, channel). 
     * fields: e.g. client_buffer,channel=abc,server_id=1
     * tag_key: e.g. channel */
    uint8_t get_tag_id(const vector<string_view> & tags, string_view tag_key, 
                       string_table& table) {
        string value_str; 
        string_view tag_prefix = string(tag_key) + "=";
        unsigned prefix_len = tag_prefix.length();
        
        for (const auto & tag : tags) {
            if (not tag.compare(0, prefix_len, tag_prefix)) {
                value_str = string(tag.substr(prefix_len));
            }
        }

        if (value_str.empty()) {
            throw runtime_error(string(tag_key) + " missing");
        }
        // Insert new value if needed
        return table.forward_map_vivify(value_str);
    }

    /* Get the numeric value of a dynamic "tag". 
     * Each measurement has a pair of tags that appears on every line,
     * e.g. {server, channel} for client_buffer/video_sent/video_acked. 
     * Since every line has these tags, tags can be used to disambiguate datapoints
     * (as opposed to the other fields e.g. cum_rebuf, which only appear on some client_buffer lines).
     * A dynamic tag can take any number of values (server tag is currently static). */
    template<typename Vec>  // Tag value represents an index into this vector 
    uint8_t get_dynamic_tag_id(Vec & vec, vector<string_view> & tags, string_view tag_key) {
        if (tag_key != "channel"sv and tag_key != "format"sv) { 
            throw runtime_error("Unknown tag key " + string(tag_key));
        }

        string_table& table = tag_key == "channel" ? channels : formats;
        // updates table as needed
        const uint8_t tag_id = get_tag_id(tags, tag_key, table);

        const unsigned n_cur_tag_values = vec.size();
        /* The size of the vector must be at least the highest tag_id seen by this server.
         * That number may increase by more than one across calls to this function with a given server_id,
         * since other servers also call get_tag_id() on the same tag, adding a new value to the global table. */
        int n_new_tag_values = tag_id + 1 - n_cur_tag_values;
        if (n_new_tag_values > 0) {
             /* Resize will only happen a few times - we only see ~10 possible values for channel and format.
              * Vectors are reserved up front, so resize will likely never incur a realloc.
              * Default-inits any holes between previous extent and new value. */
             vec.resize(n_cur_tag_values + n_new_tag_values);
        }
        return tag_id;
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
            new CryptoPP::Base64Encoder(    // CryptoPP takes ownership
                new CryptoPP::StringSink(public_ids_list.session_id),
                /* insertLineBreaks = */ false
            ) 
        ); 
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

    /* Dump an array of measurements to csv, including session ID from stream_ids.
     * For events without first_init_id, stream index was also recorded in stream_ids.
     * For events with first_init_id, stream index is calculated as init_id - first_init_id.
     * meas_arr should be some container<vector<T_table>>, where T provides anon_keys/values() and
     * has first_init_id, init_id, user_id, expt_id as optional members */
    template <typename MeasurementArray>
    void dump_private_measurement(const MeasurementArray & meas_arr, const string & meas_name) {
        const string & dump_filename = meas_name + "_" + date_str + ".csv";
        ofstream dump_file{dump_filename};
        if (not dump_file.is_open()) {
            throw runtime_error( "can't open " + dump_filename);
        }
        
        dump_file << "time (ns GMT),session_id,index,expt_id,channel,";
        bool wrote_header = false; 

        // Write all datapoints
        for (uint64_t server = 0; server < meas_arr.size(); server++) {
            for (uint8_t channel_id = 0; channel_id < meas_arr[server].size(); channel_id++) {
                for (const auto & [ts,datapoint] : meas_arr[server][channel_id]) {
                    if (datapoint.bad) {
                        bad_count++;
                        cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values "
                                "(while dumping measurements).\n";
                        continue;
                    }
                    if (not datapoint.complete()) {
                        cerr << datapoint << endl;
                        throw runtime_error("incomplete datapoint with timestamp " + to_string(ts));
                    }

                    // Write column header using the first datapoint encountered 
                    // (note there may not be a datapoint at server=channel=0)
                    if (not wrote_header) {
                        dump_file << datapoint.anon_keys() << "\n";
                        wrote_header = true;
                    }

                    // Get anonymous session/stream ID for datapoint
                    public_stream_id public_id; 
                    try {   
                        public_id = 
                            get_anonymous_ids({datapoint.first_init_id, *datapoint.init_id,
                                *datapoint.user_id, *datapoint.expt_id, server, channel_id});
                    } catch (const exception & e) {
                        /* All Events should have IDs, but the other datapoints may not -- 
                         * see comment on get_anonymous_ids */
                        if (meas_name == "client_buffer") {
                            throw;
                        }
                        cerr << "Datapoint with timestamp " << ts << " has no corresponding event: " 
                             << e.what() << "\n";
                        continue;   // don't dump this chunk
                    }
                    
                    // video_sent requires formats table to get format string
                    const string & anon_values = meas_name == "video_sent" ? 
                        datapoint.anon_values(formats) : datapoint.anon_values();
                
                    dump_file << ts << "," 
                              << public_id.session_id << ","
                              << public_id.index << ","
                              << *datapoint.expt_id << ","
                              << channels.reverse_map(channel_id) << "," 
                              << anon_values << "\n";
                }
            }
        }

        dump_file.close();   
        if (dump_file.bad()) {
            throw runtime_error("error writing " + dump_filename);
        }
    }

    /* meas_arr should be some container<vector<T_table>>, where T provides anon_keys/values(). 
     * Separate from dump_private to allow templating 
     * (private version requires private fields like init_id, which public measurements don't have) */
    template <typename MeasurementArray>
    void dump_public_measurement(const MeasurementArray & meas_arr, const string & meas_name) {
        const string & dump_filename = meas_name + "_" + date_str + ".csv";
        ofstream dump_file{dump_filename};
        if (not dump_file.is_open()) {
            throw runtime_error( "can't open " + dump_filename);
        }

        // Write column header using any datapoint (here, the first one)
        // Current non-anonymous measurements of interest (video_size, ssim) have format and channel as tags
        dump_file << "time (ns GMT),format,channel,";
        bool wrote_header = false; 
        
        // Write all datapoints
        for (uint8_t format_id = 0; format_id < meas_arr.size(); format_id++) {
            for (uint8_t channel_id = 0; channel_id < meas_arr.at(format_id).size(); channel_id++) {
                for (const auto & [ts,datapoint] : meas_arr[format_id][channel_id]) {
                    if (datapoint.bad) {
                        bad_count++;
                        cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values "
                                "(while dumping measurements).\n";
                        continue;
                    }
                    if (not datapoint.complete()) {
                        cerr << datapoint << endl;
                        throw runtime_error("incomplete datapoint with timestamp " + to_string(ts));
                    }

                    // Write column header using the first datapoint encountered 
                    // (note there may not be a datapoint at server=channel=0)
                    if (not wrote_header) {
                        dump_file << datapoint.anon_keys() << "\n";
                        wrote_header = true;
                    }
                    
                    dump_file << ts << ","
                              << formats.reverse_map(format_id) << ","
                              << channels.reverse_map(channel_id) << "," 
                              << datapoint.anon_values() << "\n";
                }
            }
        }

        dump_file.close();   
        if (dump_file.bad()) {
            throw runtime_error("error writing " + dump_filename);
        }
    }
    
    public:

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
                        // bad_count++ happens in dump(), for all datapoints
                        cerr << "Skipping bad data point (of " << bad_count 
                             << " total) with contradictory values (while grouping stream IDs).\n";
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

    Parser(Day_ns start_ts, const string & date_str) : date_str(date_str)
    {
        usernames.forward_map_vivify("unknown");
        browsers.forward_map_vivify("unknown");
        ostable.forward_map_vivify("unknown");
        formats.forward_map_vivify("unknown");
        channels.forward_map_vivify("unknown");

        days.first = start_ts;
        days.second = start_ts + 60 * 60 * 24 * NS_PER_SEC;

        // reserve vectors (will likely never need to realloc)
        for (auto & vec : client_buffer) vec.reserve(N_CHANNELS_ESTIMATE);
        for (auto & vec : video_sent) vec.reserve(N_CHANNELS_ESTIMATE);
        for (auto & vec : video_acked) vec.reserve(N_CHANNELS_ESTIMATE);
        // If data contains more formats than estimated, inner vectors beyond the estimate won't be reserved.
        // This should be rare.
        for (auto & inner_vec : video_size) inner_vec.reserve(N_CHANNELS_ESTIMATE);
        for (auto & inner_vec : ssim) inner_vec.reserve(N_CHANNELS_ESTIMATE);
    }
    
    /* To dump a new measurement: 
     * 1) Populate measurement array during parse() (add tag key to get_dynamic_tag_id() for new tag)
     * 2) Define struct 
     * 3) Call dump_*_measurement() */
    void dump_all_measurements() {
        dump_private_measurement(client_buffer, VAR_NAME(client_buffer)); 
        dump_private_measurement(video_sent, VAR_NAME(video_sent));
        dump_private_measurement(video_acked, VAR_NAME(video_acked));
        dump_public_measurement(video_size, VAR_NAME(video_size));
        dump_public_measurement(ssim, VAR_NAME(ssim));
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
                    const uint64_t server_id = get_server_id(measurement_tag_set_fields);
                    const uint8_t channel_id = get_dynamic_tag_id(client_buffer.at(server_id), 
                                                                  measurement_tag_set_fields, 
                                                                  "channel"sv);
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
                    const uint8_t format_id = get_dynamic_tag_id(ssim,
                                                                 measurement_tag_set_fields, 
                                                                 "format"sv);
                    const uint8_t channel_id = get_dynamic_tag_id(ssim.at(format_id),
                                                                  measurement_tag_set_fields, 
                                                                  "channel"sv);
                    ssim.at(format_id).at(channel_id)[timestamp].insert_unique(key, value);
                } else if ( measurement == "video_acked"sv ) {
                    const uint64_t server_id = get_server_id(measurement_tag_set_fields);
                    const uint8_t channel_id = get_dynamic_tag_id(video_acked.at(server_id), 
                                                                  measurement_tag_set_fields, 
                                                                  "channel"sv);
                    video_acked[server_id].at(channel_id)[timestamp].insert_unique(key, value, usernames);
                } else if ( measurement == "video_sent"sv ) {
                    const uint64_t server_id = get_server_id(measurement_tag_set_fields);
                    const uint8_t channel_id = get_dynamic_tag_id(video_sent.at(server_id), 
                                                                  measurement_tag_set_fields, 
                                                                  "channel"sv);
                    video_sent[server_id].at(channel_id)[timestamp].insert_unique(key, value, usernames, formats);
                } else if ( measurement == "video_size"sv ) {
                    const uint8_t format_id = get_dynamic_tag_id(video_size,
                                                                 measurement_tag_set_fields, 
                                                                 "format"sv);
                    const uint8_t channel_id = get_dynamic_tag_id(video_size.at(format_id),
                                                                  measurement_tag_set_fields, 
                                                                  "channel"sv);
                    video_size.at(format_id).at(channel_id)[timestamp].insert_unique(key, value);
                } else {
                    throw runtime_error( "Can't parse: " + string(line) );
                }
            } catch (const exception & e ) {
                cerr << "Failure on line: " << line << "\n";
                throw;
            }
        }
    }
};  // end Parser

void private_analyze_main(const string & date_str, Day_ns start_ts) {
    // use date_str to name csv
    Parser parser{ start_ts, date_str };
    parser.parse_stdin();
    parser.group_stream_ids();
    parser.anonymize_stream_ids(); 
    // parser.check_public_stream_id_uniqueness(); // remove (test only)
    parser.dump_all_measurements();
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
