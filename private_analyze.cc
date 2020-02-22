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

using event_table = map<uint64_t, Event>;
using sysinfo_table = map<uint64_t, Sysinfo>;
using video_sent_table = map<uint64_t, VideoSent>;

using private_stream_id = tuple<uint32_t, uint32_t, uint32_t>;
/*                              init_id,  uid,      expt_id */
using stream_ids_table = map<private_stream_id, public_stream_id>;
typedef map<private_stream_id, public_stream_id>::iterator stream_ids_iterator;


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

        // client_buffer[server] = map<ts, Event>
        // server helps disambiguate in case two events are
        // received with identical timestamp
        array<event_table, SERVER_COUNT> client_buffer{};
        
        // client_sysinfo[server] = map<ts, SysInfo>
        array<sysinfo_table, SERVER_COUNT> client_sysinfo{};
        
        // video_sent[server] = map<ts, VideoSent>
        array<video_sent_table, SERVER_COUNT> video_sent{}; 

        stream_ids_table stream_ids{};
        
        vector<string> experiments{};

        unsigned int bad_count = 0;
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
        {
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

                        /* If timestamps within a server_id aren't unique, Event will become contradictory
                         * and we'll record it as "bad" later (hasn't happened in data thru 2/2/20). */
                        client_buffer[server_id][timestamp].insert_unique(key, value, usernames);
                        // set channel (not unique, since provided with every datapoint)
                        client_buffer[server_id][timestamp].channel = channel;  
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
                        const auto server_id = get_server_id(measurement_tag_set_fields);
                        const auto channel = get_channel(measurement_tag_set_fields);
                        video_sent[server_id][timestamp].insert_unique(key, value, usernames);
                        video_sent[server_id][timestamp].channel = channel;  
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
        void record_new_session(const private_stream_id& private_id) {
            /* Generate session id in-place, record with channel_changes = 0 */
            pair<private_stream_id, public_stream_id> stream_id_mapping(private_id, public_stream_id{});
            pair<stream_ids_iterator, bool> insertion = stream_ids.insert(stream_id_mapping);
            public_stream_id& public_stream_id = insertion.first->second;
            int entropy_ret = getentropy(public_stream_id.session_id.data(), BYTES_OF_ENTROPY);
            if (entropy_ret != 0) {
                throw runtime_error(string("Failed to generate public session ID: ") + strerror(errno));
            }
            for (char & c : public_stream_id.session_id) {
                // TODO: hack to display special csv characters: comma, CR, LF, or double quote (RFC 4180)
                if (c == ',' || c == '\r' || c == '\n' || c == '\"') {
                    c = 'a';
                }
            }
        }

        /* Assign cryptographically secure session ID to each stream, and record channel_changes
         * (which serve as a public stream ID, in combination with session ID) */
        void anonymize_stream_ids() {
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                // const size_t rss = memcheck() / 1024;        // TODO: put back
                // cerr << "server " << int(server) << "/" << client_buffer.size() << ", RSS=" << rss << " MiB\n";
                // iterates in increasing ts order
                for (const auto & [ts,event] : client_buffer[server]) {
                    if (event.bad) {
                        bad_count++;
                        cerr << "Skipping bad data point (of " << bad_count << " total) with contradictory values.\n";
                        // TODO: remove (check if removing channel from client_buffer caused any bad events)
                        cerr << event;
                        throw runtime_error("event with contradictory values");
                        continue;
                    }
                    if (not event.complete()) {
                        throw runtime_error("incomplete event with timestamp " + to_string(ts));
                    }

                    optional<uint32_t> first_init_id = event.first_init_id;
                    // Record with first_init_id, if available
                    private_stream_id private_id = {first_init_id.value_or(*event.init_id), *event.user_id, *event.expt_id};
                    // Check if this stream has already been recorded (via another Event in the stream)
                    stream_ids_iterator found_stream_id = stream_ids.find(private_id);
                    if (found_stream_id != stream_ids.end()) {
                        cerr << "already recorded session/stream for event with ts " << to_string(ts) << endl; // TODO: remove
                        continue; 
                    }
                    if (first_init_id) {
                        /* No need to decrement -- if stream has first_init_id, record with that */
                        /* Haven't recorded this session yet => generate session id and add to map */
                        record_new_session(private_id);
                        // if already recorded, done (will calculate channel_changes on dump)
                    } else {
                        /* client_buffer *is* ordered by timestamp within server (and therefore within session), 
                         * so can decrement to find stream in stream_ids recorded thus far, since session
                         * will already have been recorded if this is not the first stream in the session */
                        unsigned channel_changes = 0;
                        // already searched for exact match -- start with decrement = 1
                        for ( unsigned int decrement = 1; decrement < 1024; decrement++ ) {
                            found_stream_id = stream_ids.find({get<0>(private_id) - decrement,
                                    get<1>(private_id),
                                    get<2>(private_id)});
                            if (found_stream_id == stream_ids.end()) {
                                // loop again
                            } else {
                                channel_changes = decrement;
                                break;
                            }
                        }
                        if (found_stream_id == stream_ids.end()) {
                            /* This is the first stream in session -- generate session id and add to map */
                            record_new_session(private_id);
                        } else {
                            /* Already recorded this session (but not this stream) => 
                             * copy other stream's session id, record channel_changes, add to map */
                            public_stream_id& found_public_id = found_stream_id->second;
                            public_stream_id new_public_id{found_public_id.session_id, channel_changes};  
                            stream_ids[private_id] = new_public_id;
                        }
                    }    
                }   // end client_buffer[server] loop 
            }   // end client_buffer loop
        }

        void debug_print_stream_ids() {
            // check channels, session_id, channel changes
            cerr << "Events and their IDs:" << endl;
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (const auto & [ts,event] : client_buffer[server]) {
                    cerr << "\n" << event;
                    optional<uint32_t> first_init_id = event.first_init_id;
                    private_stream_id private_id = {first_init_id.value_or(*event.init_id), *event.user_id, *event.expt_id};
                    stream_ids_iterator found_stream_id = stream_ids.find(private_id);
                    assert (found_stream_id != stream_ids.end());   // every stream should have been assigned an ID

                    public_stream_id public_id = found_stream_id->second;
                    cerr << "session ID " << public_id.session_id.data()
                         << "\nchannel changes (always 0 if has first_init_id) " << public_id.channel_changes << endl;
                }
            }
        }

        // lines won't be ordered by ts across server_ids
        void dump_anonymized_data(const string & date_str) {

            // TODO: remove (test only)
            debug_print_stream_ids();

            // CLIENT BUFFER
            // line format:
            // ts session_id channel_changes expt_id channel <event fields> 
            const string & client_buffer_filename = "client_buffer_" + date_str + ".csv";
            ofstream client_buffer_file;
            client_buffer_file.open(client_buffer_filename);
            if (not client_buffer_file.is_open()) {
                throw runtime_error( "can't open " + client_buffer_filename);
            }
            client_buffer_file << "time (ns GMT),session_id,channel_changes,expt_id,channel,event,buffer,cum_rebuf\n";
            for (uint8_t server = 0; server < client_buffer.size(); server++) {
                for (const auto & [ts,event] : client_buffer[server]) {
                    // already threw for incomplete events when anonymizing
                    // TODO: for videosent/acked/sysinfo, check bad/complete when dumping
                    if (event.bad) continue;
                    optional<uint32_t> first_init_id = event.first_init_id;
                    // Look up anonymous session/stream ID
                    private_stream_id private_id = {first_init_id.value_or(*event.init_id), *event.user_id, *event.expt_id};
                    stream_ids_iterator found_stream_id = stream_ids.find(private_id);
                    if (found_stream_id == stream_ids.end()) {
                        // This shouldn't happen -- all events should have IDs by now
                        throw runtime_error( "Failed to find anonymized session/stream ID for event with init_id " 
                                              + *event.init_id );
                    }
                    
                    public_stream_id& public_id = found_stream_id->second;
                    int channel_changes = public_id.channel_changes;
                    if (first_init_id) {
                        // If event has first_init_id, its session only appears once in the map,
                        // recorded with channel_changes = 0 => calculate channel_changes 
                        channel_changes = *event.init_id - *first_init_id;
                    }
  
                    client_buffer_file << ts << ",";
                    // write session_id as raw bytes
                    client_buffer_file.write(public_id.session_id.data(), BYTES_OF_ENTROPY);
                    client_buffer_file << "," << channel_changes
                                       << "," << *event.expt_id << "," << *event.channel << "," 
                                       << string_view(*event.type) << "," << *event.buffer << "," << *event.cum_rebuf << "\n";
                }
            }

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
            }
        }
        // VIDEO SENT
        // line format:
        // ts session_id channel_changes expt_id channel <videosent fields>
        // VIDEO ACKED
        // line format:
        // ts session_id channel_changes expt_id channel video_ts
        // TODO: also dump sysinfo

};  // end Parser

void private_analyze_main(const string & experiment_dump_filename, const string & date_str,
                          Day_ns start_ts) {
    Parser parser{ experiment_dump_filename, start_ts };

    parser.parse_stdin();
    parser.anonymize_stream_ids();
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

        if (argc != 3) {
            cerr << "Usage: " << argv[0] << " expt_dump [from postgres] date [e.g. 2019-07-01T11_2019-07-02T11]\n";
            return EXIT_FAILURE;
        }

        optional<Day_ns> start_ts = parse_date(argv[2]); 
        if (not start_ts) {
            cerr << "Date argument could not be parsed; format as 2019-07-01T11_2019-07-02T11\n";
            return EXIT_FAILURE;
        }
        
        private_analyze_main(argv[1], argv[2], start_ts.value());
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
