AM_CPPFLAGS = $(CXX17_FLAGS) $(jemalloc_CFLAGS) $(jsoncpp_CFLAGS)
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = influx_to_csv csv_to_stream_stats stream_to_scheme_stats stream_stats_to_metadata

influx_to_csv_SOURCES = influx_to_csv.cc
influx_to_csv_LDADD = $(jemalloc_LIBS) $(CRYPTO_LIBS)

csv_to_stream_stats_SOURCES = csv_to_stream_stats.cc
csv_to_stream_stats_LDADD = $(jsoncpp_LIBS) $(jemalloc_LIBS)

stream_to_scheme_stats_SOURCES = stream_to_scheme_stats.cc
stream_to_scheme_stats_LDADD = $(jemalloc_LIBS)

stream_stats_to_metadata_SOURCES = stream_stats_to_metadata.cc
