#ifndef WATCHTIMESUTIL_HH
#define WATCHTIMESUTIL_HH

// min and max acceptable watch time bin indices (inclusive)
constexpr static unsigned int MIN_BIN = 2;
constexpr static unsigned int MAX_BIN = 20;
// Maximum number of bins (i.e. number of elements in each array used to 
// represent bins)
constexpr static unsigned int MAX_N_BINS = 32;
static_assert(MAX_BIN < MAX_N_BINS);


#endif
