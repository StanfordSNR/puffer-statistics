AM_CPPFLAGS = $(CXX17_FLAGS) $(jemalloc_CFLAGS)
AM_CXXFLAGS = $(PICKY_CXXFLAGS)

bin_PROGRAMS = parser

parser_SOURCES = parser.cc
parser_LDADD = $(jemalloc_LIBS)
