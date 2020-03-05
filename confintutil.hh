/* Utilities useful for (pre)confinterval */

#ifndef CONFINTUTIL_HH
#define CONFINTUTIL_HH

// Number of statistics fields output per stream 
constexpr static unsigned int N_STREAM_STATS = 14;

// Max length of stream statistics line 
constexpr static unsigned int MAX_LINE_LEN = 500;

// min and max acceptable watch time bin indices (inclusive)
constexpr static unsigned int MIN_BIN = 2;
constexpr static unsigned int MAX_BIN = 20;

// Maximum number of bins (i.e. number of elements in each array used to 
// represent bins)
constexpr static unsigned int MAX_N_BINS = 32;

static_assert(MAX_BIN < MAX_N_BINS);

bool stream_is_slow(double delivery_rate) {
    return delivery_rate <= (6000000.0/8.0);
}

#endif
