AM_CPPFLAGS = $(CXX17_FLAGS) $(jemalloc_CFLAGS) $(jsoncpp_CFLAGS)
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = parser private_analyze public_analyze submission_anon_analyze confinterval schemedays

schemedays_SOURCES = schemedays.cc

parser_SOURCES = parser.cc
parser_LDADD = $(jemalloc_LIBS)

public_analyze_SOURCES = public_analyze.cc
public_analyze_LDADD = $(jsoncpp_LIBS) $(jemalloc_LIBS)

private_analyze_SOURCES = private_analyze.cc
private_analyze_LDADD = $(jemalloc_LIBS)

submission_anon_analyze_SOURCES = submission_anon_analyze.cc
submission_anon_analyze_LDADD = $(jsoncpp_LIBS) $(jemalloc_LIBS)

confinterval_SOURCES = confinterval.cc
confinterval_LDADD = $(jemalloc_LIBS)
