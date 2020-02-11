AM_CPPFLAGS = $(CXX17_FLAGS) $(jemalloc_CFLAGS) $(jsoncpp_CFLAGS)
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = parser analyze confinterval schemedays

schemedays_SOURCES = schemedays.cc

parser_SOURCES = parser.cc
parser_LDADD = $(jemalloc_LIBS)

analyze_SOURCES = analyze.cc
analyze_LDADD = $(jsoncpp_LIBS) $(jemalloc_LIBS)

confinterval_SOURCES = confinterval.cc
confinterval_LDADD = $(jemalloc_LIBS)
