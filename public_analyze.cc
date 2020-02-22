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

    public:
        Parser()
            : streams(), sysinfos(), chunks()
        {
            // TODO: check sysinfo/chunks empty keys are used ; may need elem-by-elem print
            streams.set_empty_key( { {0}, -1U} );    // we never insert a stream with channel_changes -1
            sysinfos.set_empty_key({0,0,0});
            chunks.set_empty_key({{0},0});

            usernames.forward_map_vivify("unknown");
            browsers.forward_map_vivify("unknown");
            ostable.forward_map_vivify("unknown");
        }

        /* Read input file into streams map */
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
           
            cout << "\nParsed events:\n" << endl;
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
                cout << "ts " << ts << endl; 
                cout << "channel changes " << stream_id.channel_changes << endl;
                cout << "session ID " << stream_id.session_id.data() << endl;
                cout << event;
                stream_key key = make_tuple(stream_id.session_id, stream_id.channel_changes);
                streams[key].push_back({ts, event});   // allocate event 
            } 

            client_buffer_file.close();   
            if (client_buffer_file.bad()) {
                throw runtime_error("error writing " + client_buffer_filename);
            }

        }
};

void public_analyze_main(const string & date_str) {
    Parser parser{};
    parser.parse_client_buffer_input(date_str);
}

int main(int argc, char *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 2) {
            cerr << "Usage: " << argv[0] << " date [e.g. 2019-07-01T11_2019-07-02T11]\n";
            return EXIT_FAILURE;
        }

        public_analyze_main(argv[1]);
    } catch (const exception & e) {
        cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
