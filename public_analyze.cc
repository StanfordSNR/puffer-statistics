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

/* 
 * Read in anonymized data (grouped by timestamp/server/channel), into data structures 
 * grouped by stream. 
 * To stdout, outputs summary of each stream (one stream per line).
 * Takes experimental settings and date as arguments.
 */

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
        // Convert format string to uint8_t for storage
        string_table formats{};
        
        // streams[public_stream_id] = vec<[ts, Event]>
        using stream_key = tuple<public_session_id, unsigned>;   // unpack struct for hash
        /*                                          index */
        dense_hash_map<stream_key, vector<pair<uint64_t, Event>>, boost::hash<stream_key>> streams;

        // sysinfos[sysinfo_key] = SysInfo
        using sysinfo_key = tuple<uint32_t, uint32_t, uint32_t>;
        /*                        init_id,  uid,      expt_id */
        dense_hash_map<sysinfo_key, Sysinfo, boost::hash<sysinfo_key>> sysinfos;

        // chunks[public_stream_id] = vec<[ts, VideoSent]>
        dense_hash_map<stream_key, vector<pair<uint64_t, const VideoSent>>, boost::hash<stream_key>> chunks;

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
            // TODO: check sysinfo empty key
            streams.set_empty_key( {{0}, -1U} );    // we never insert a stream with index -1
            sysinfos.set_empty_key({0,0,0});
            chunks.set_empty_key( {{0}, -1U} );    
            formats.forward_map_vivify("unknown");

            read_experimental_settings_dump(experiment_dump_filename);
        }

        /* Read anonymized client_buffer input file into streams map. 
         * Each line of input is one event datapoint, recorded with its public stream ID. */
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
           
            unsigned line_no = 0;
            while (getline(client_buffer_file, line_storage)) {
                if (line_no % 1000000 == 0) {
                    const size_t rss = memcheck() / 1024;
                    cerr << "line " << line_no / 1000000 << "M, RSS=" << rss << " MiB\n"; 
                }
                line_no++;

                istringstream line(line_storage);
                if (not (line >> ts >> comma and comma == ',' and                
                         // read stream id as raw bytes
                         line.read(stream_id.session_id.data(), BYTES_OF_ENTROPY) and 
                         line >> comma and comma == ',' and 
                         line >> stream_id.index >> comma and comma == ',' and 
                         line >> expt_id >> comma and comma == ',' and
                         getline(line, channel, ',') and getline(line, event_type_str, ',') and
                         line >> buffer >> comma and comma == ',' and line >> cum_rebuf) ) {
                    throw runtime_error("error reading from " + client_buffer_filename);
                }
                
                // no need to fill in private fields
                Event event{nullopt, nullopt, expt_id, nullopt, string_view(event_type_str), 
                            buffer, cum_rebuf};

                /* Add event to list of events corresponding to its stream */
                streams[{stream_id.session_id, stream_id.index}].emplace_back(make_pair(ts, event));   // allocates event 
            } 

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
            }
        }
        
        /* Read anonymized video_sent input file into chunks map. 
         * Each line of input is one chunk, recorded with its public stream ID. */
        void parse_video_sent_input(const string & date_str) {
            const string & video_sent_filename = "video_sent_" + date_str + ".csv";
            ifstream video_sent_file{video_sent_filename};
            if (not video_sent_file.is_open()) {
                throw runtime_error( "can't open " + video_sent_filename);
            }
            
            string line_storage;
            char comma;
            uint64_t ts, video_ts; 
            public_stream_id stream_id;
            // can't read directly into optional
            string channel, format;
            float ssim_index;
            uint32_t delivery_rate, expt_id, size, cwnd, in_flight, min_rtt, rtt;
            
            // ignore column labels
            getline(video_sent_file, line_storage);

            unsigned line_no = 0;
            while (getline(video_sent_file, line_storage)) {
                if (line_no % 1000000 == 0) {
                    const size_t rss = memcheck() / 1024;
                    cerr << "line " << line_no / 1000000 << "M, RSS=" << rss << " MiB\n"; 
                }
                line_no++;

                istringstream line(line_storage);
                if (not (line >> ts >> comma and comma == ',' and                
                         // read stream id as raw bytes
                         line.read(stream_id.session_id.data(), BYTES_OF_ENTROPY) and 
                         line >> comma and comma == ',' and 
                         line >> stream_id.index >> comma and comma == ',' and 
                         line >> expt_id >> comma and comma == ',' and
                         getline(line, channel, ',') and 
                         line >> video_ts >> comma and comma == ',' and 
                         getline(line, format, ',') and 
                         line >> size >> comma and comma == ',' and
                         line >> ssim_index >> comma and comma == ',' and
                         line >> cwnd >> comma and comma == ',' and
                         line >> in_flight >> comma and comma == ',' and 
                         line >> min_rtt >> comma and comma == ',' and
                         line >> rtt >> comma and comma == ',' and
                         line >> delivery_rate) ) {
                    throw runtime_error("error reading from " + video_sent_filename);
                }
                // leave private fields blank
                VideoSent video_sent{ssim_index = ssim_index, delivery_rate, expt_id, nullopt, nullopt, nullopt,
                    size, formats.forward_map_vivify(format), cwnd, in_flight, min_rtt, rtt, video_ts};

                /* Add chunk to list of chunks corresponding to its stream */
                chunks[{stream_id.session_id, stream_id.index}].emplace_back(make_pair(ts, video_sent));   
            } 

            video_sent_file.close();   
            if (video_sent_file.bad()) {
                throw runtime_error("error writing " + video_sent_filename);
            }
        }
       
        void print_grouped_data() {
            cerr << "streams:" << endl;
            for ( const auto & [stream_id, events] : streams ) {
                const auto & [session_id, index] = stream_id;
                cerr << "public session ID (first two char): " << session_id[0] << session_id[1] << endl;
                cerr << ", stream index: " << index << endl;
                for ( const auto & [ts, event] : events ) {
                    cerr << ts << ", " << event; 
                }
            }            
            // Count total events, streams
            size_t n_total_events = 0;
            for ( auto & [unpacked_stream_id, events] : streams ) {
                n_total_events += events.size();
            }
            cerr << "n_total_events " << n_total_events << endl;
            cerr << "n_total_streams " << streams.size() << endl;
            cerr << "chunks:" << endl;
            for ( const auto & [stream_id, stream_chunks] : chunks ) {
                const auto & [session_id, index] = stream_id;
                cerr << "public session ID (first two char): " << session_id[0] << session_id[1] << endl;
                cerr << ", stream index: " << index << endl;
                for ( const auto & [ts, video_sent] : stream_chunks ) {
                    cerr << ts << ", " << video_sent; 
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
               
                /* find matching videosent stream */
                const auto [normal_ssim_chunks, ssim_1_chunks, total_chunks, ssim_sum, 
                            mean_delivery_rate, average_bitrate, ssim_variation] = video_summarize(unpacked_stream_id);
                const double mean_ssim = ssim_sum == -1 ? -1 : ssim_sum / normal_ssim_chunks;
                const size_t high_ssim_chunks = total_chunks - normal_ssim_chunks;

                if (mean_delivery_rate < 0 ) {
                    missing_video_stats++;
                } else {
                    overall_chunks += total_chunks;
                    overall_high_ssim_chunks += high_ssim_chunks;
                    overall_ssim_1_chunks += ssim_1_chunks;
                }

                cout << fixed;

                // ts in anonymized data include nanoseconds -- truncate to seconds
                cout << "ts=" << (summary.base_time / 1000000000) 
                     << " valid=" << (summary.valid ? "good " : "bad ") 
                     << " full_extent=" << (summary.full_extent ? "full " : "trunc " ) 
                     << " bad_reason=" << summary.bad_reason << " "
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

            const vector<pair<uint64_t, const VideoSent>> & chunk_stream = videosent_it->second;

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
                float raw_ssim = videosent.ssim_index.value(); // would've thrown by this point if not set
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

                delivery_rate_sum += videosent.delivery_rate.value();
                bytes_sent_sum += videosent.size.value();
            }

            const double average_bitrate = 8 * bytes_sent_sum / (2.002 * chunk_stream.size());

            double average_absolute_ssim_variation = -1;
            if (num_ssim_var_samples > 0) {
                average_absolute_ssim_variation = ssim_absolute_variation_sum / num_ssim_var_samples;
            }

            return { num_ssim_samples, num_ssim_1_chunks, chunk_stream.size(), ssim_sum, delivery_rate_sum / chunk_stream.size(), average_bitrate, average_absolute_ssim_variation };
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
    parser.parse_video_sent_input(date_str);
    parser.analyze_streams(); 
}

/* Date is used to name csvs. */
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
