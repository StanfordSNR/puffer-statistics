#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([puffer-analysis], [0.1], [keithw@cs.stanford.edu])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_HEADERS([config.h])

# Add picky CXXFLAGS
CXX17_FLAGS="-std=c++17"
PICKY_CXXFLAGS="-Wpedantic -Wall -Wextra -Weffc++ -Werror"
AC_SUBST([CXX17_FLAGS])
AC_SUBST([OPT_CXXFLAGS])
AC_SUBST([PICKY_CXXFLAGS])

# Change default CXXflags
: ${CXXFLAGS="-g -Ofast -march=native -mtune=native"}

# Checks for programs.
AC_PROG_CXX
AC_PROG_RANLIB

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([
    Makefile
])
AC_OUTPUT
