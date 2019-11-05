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
AC_SUBST([PICKY_CXXFLAGS])

# Change default CXXflags
: ${CXXFLAGS="-g -Ofast -march=native -mtune=native"}

# Checks for programs.
AC_PROG_CXX
AC_PROG_RANLIB

# Checks for libraries.
PKG_CHECK_MODULES([jemalloc],[jemalloc])
PKG_CHECK_MODULES([jsoncpp], [jsoncpp])
PKG_CHECK_MODULES([CRYPTO],[libcrypto++])

# Checks for header files.
AC_LANG_PUSH(C++)
save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$CXX17_FLAGS"
AC_CHECK_HEADERS([google/dense_hash_map], [], [AC_MSG_ERROR([Missing required header file.])])
AC_CHECK_HEADERS([google/sparse_hash_map], [], [AC_MSG_ERROR([Missing required header file.])])
CPPFLAGS="$save_CPPFLAGS"
AC_LANG_POP(C++)

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([
    Makefile
])
AC_OUTPUT
