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

/* Read in anonymized data (grouped by timestamp), into data structures 
 * grouped by stream.
 * Use timestamps in seconds.
 */

// TODO: remove unnecc fcts

using event_table = map<uint64_t, Event>;
using sysinfo_table = map<uint64_t, Sysinfo>;
using video_sent_table = map<uint64_t, VideoSent>;
/* Whenever a timestamp is used to represent a day, round down to Influx backup hour.
 * Timestamps in input are nanoseconds - use nanoseconds up until writing ts to stdout. */
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

        // streams[public_stream_id] = vec<[ts, Event]>
        using stream_key = tuple<public_session_id, unsigned>;   // unpack struct for hash
        /*                                          channel_changes */
        dense_hash_map<stream_key, vector<pair<uint64_t, Event>>, boost::hash<stream_key>> streams;

        // sysinfos[sysinfo_key] = SysInfo
        using sysinfo_key = tuple<uint32_t, uint32_t, uint32_t>;
        /*                        init_id,  uid,      expt_id */
        dense_hash_map<sysinfo_key, Sysinfo, boost::hash<sysinfo_key>> sysinfos;

        // chunks[public_stream_id] = vec<[ts, VideoSent]>
        dense_hash_map<public_stream_id, vector<pair<uint64_t, const VideoSent*>>, boost::hash<public_stream_id>> chunks;

        unsigned int bad_count = 0;
        
        // Used in summarizing stream, to convert numeric experiment ID to scheme string
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
                // populate experiments with expt_id => abr_name/cc or abr/cc
                experiments.at(experiment_id) = name + "/" + doc["cc"].asString();
            }
        }


    public:
        Parser(const string & experiment_dump_filename)
            : streams(), sysinfos(), chunks()
        {
            // TODO: check sysinfo/chunks empty keys
            streams.set_empty_key( {{0}, -1U} );    // we never insert a stream with channel_changes -1
            sysinfos.set_empty_key({0,0,0});
            chunks.set_empty_key({{0},0});

            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");
            
            read_experimental_settings_dump(experiment_dump_filename);
        }

        /* Read anonymized client_buffer input file into streams map. 
         * Each line of input is one event, recorded with its public stream ID. */
        void parse_client_buffer_input(const string & date_str) {
            const string & client_buffer_filename = "client_buffer_" + date_str + ".csv";
            ifstream client_buffer_file{client_buffer_filename};
            if (not client_buffer_file.is_open()) {
                throw runtime_error( "can't open " + client_buffer_filename);
            }

            string line_storage;
            char comma;
            uint64_t ts; 
            public_stream_id stream_id;
            // can't read directly into optional
            uint32_t expt_id;
            string channel, event_type_str;
            float buffer, cum_rebuf;
            
            // ignore column labels
            getline(client_buffer_file, line_storage);
           
            cerr << "\nParsed events:\n" << endl;
            while (getline(client_buffer_file, line_storage)) {

                istringstream line(line_storage);

                // read ts
                if (not (line >> ts >> comma and                
                         // read stream id as raw bytes
                         line.read(stream_id.session_id.data(), BYTES_OF_ENTROPY) and
                         line >> comma >> stream_id.channel_changes >> comma >> expt_id >> comma and 
                         getline(line, channel, ',') and getline(line, event_type_str, ',') and
                         line >> buffer >> comma >> cum_rebuf ) ) {
                    throw runtime_error("error reading from " + client_buffer_filename);
                }
                
                // no need to fill in private fields
                Event event{nullopt, nullopt, expt_id, nullopt, string_view(event_type_str), 
                            buffer, cum_rebuf, channel};
                cerr << "ts " << ts << endl; 
                cerr << "channel changes " << stream_id.channel_changes << endl;
                cerr << "session ID " << stream_id.session_id.data() << endl;
                cerr << event;
                stream_key key = make_tuple(stream_id.session_id, stream_id.channel_changes);
                /* Add event to list of events corresponding to its stream */
                streams[key].push_back({ts, event});   // allocates event 
            } 

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
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
            // XXX: not outputting session ID (confinterval doesn't use it)

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
            
            for ( auto & [unpacked_stream_id, events] : streams ) {
                const EventSummary summary = summarize(events);
                // cerr << "stream summary:\n" << summary.base_time << summary.scheme << endl;
                cout << fixed;

                // ts from influx export include nanoseconds -- truncate to seconds
                // cerr all summary values
                cout << (summary.base_time / 1000000000) << " " << (summary.valid ? "good " : "bad ") << (summary.full_extent ? "full " : "trunc " ) << summary.bad_reason << " "
                    << summary.scheme << " extent=" << summary.time_extent
                    << " used=" << 100 * summary.time_at_last_play / summary.time_extent << "%"
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

        /* Summarize a list of events corresponding to a stream. */
        EventSummary summarize(const vector<pair<uint64_t, Event>> & events) const {
            EventSummary ret;
            ret.scheme = experiments.at(events.front().second.expt_id.value());   // All events in stream have same expt_id
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
               
                if (event.buffer.value() > 0.3) {
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

                if (event.buffer.value() > 5 and last_buffer > 5) {
                    if (event.cum_rebuf.value() > last_cum_rebuf + 0.15) {
                        // stall with plenty of buffer --> slow decoder?
                        ret.bad_reason = "stall_while_playing";
                        return ret; // BAD
                    }
                }
                
                switch (event.type.value().type) {
                    case Event::EventType::Type::init:
                        break;
                    case Event::EventType::Type::play:
                        playing = true;
                        ret.time_at_last_play = relative_time;
                        ret.cum_rebuf_at_last_play = event.cum_rebuf.value();
                        break;
                    case Event::EventType::Type::startup:
                        if ( not started ) {
                            ret.time_at_startup = relative_time;
                            ret.cum_rebuf_at_startup = event.cum_rebuf.value();
                            started = true;
                        }

                        playing = true;
                        ret.time_at_last_play = relative_time;
                        ret.cum_rebuf_at_last_play = event.cum_rebuf.value();
                        break;
                    case Event::EventType::Type::timer:
                        if ( playing ) {
                            ret.time_at_last_play = relative_time;
                            ret.cum_rebuf_at_last_play = event.cum_rebuf.value();
                        }
                        break;
                    case Event::EventType::Type::rebuffer:
                        playing = false;
                        break;
                }

                last_sample = relative_time;
                last_buffer = event.buffer.value();
                last_cum_rebuf = event.cum_rebuf.value();
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

void public_analyze_main(const string & experiment_dump_filename, const string & date_str) {
    Parser parser{experiment_dump_filename};
    parser.parse_client_buffer_input(date_str);
    parser.analyze_streams();
}

int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            cerr << "Usage: " << argv[0] << " expt_dump [from postgres] date [e.g. 2019-07-01T11_2019-07-02T11]\n";
            return EXIT_FAILURE;
        }

        public_analyze_main(argv[1], argv[2]);
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
