AC_PREREQ([2.63])
AC_INIT([fintp_connector], [0.0.1])
AC_CONFIG_SRCDIR([src/ConnectorMain.cpp])
AC_CONFIG_MACRO_DIR([../fintp_utils/m4])
AC_CONFIG_HEADERS([config.h])

AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

AM_INIT_AUTOMAKE

AC_CHECK_HEADERS([malloc.h stdlib.h string.h sys/param.h sys/timeb.h unistd.h], [], [Cannot build])

AC_CHECK_FUNCS([ftime gethostbyaddr gethostbyname gethostname memset strerror strtol strtoul uname], [], [Cannot build])

ENVIRONMENT

AX_PTHREAD(, [AC_MSG_ERROR([Cannot build without pthread]) ])

AC_SEARCH_LIBS([TIFFOpen], [tiffxx], AC_MSG_NOTICE([Found tiff]), \
AC_MSG_ERROR([Cannot build without libtiff]))

AC_ARG_WITH(tiff,
	    AS_HELP_STRING([--with-tiff=DIR],[Tiff location]),
	    [TIFF_HOME=$withval;tiffp=yes],tiffp=no)
if test "$tiffp" == "yes";
then
 CPPFLAGS="${CPPFLAGS} -I${TIFF_HOME}"
 LDFLAGS="${LDFLAGS} -L${TIFF_HOME}"
fi

AC_CHECK_HEADERS([tiff.h], [] , AC_MSG_ERROR([You have libtiff but not Tiff headers]))

AX_BOOST_BASE([1.48.0], , [AC_MSG_ERROR([Cannot build without Boost])])

AX_BOOST_REGEX

AX_LIB_XERCES([2.8.0])

if test $HAVE_XERCES != yes; then
	AC_MSG_ERROR([Cannot build without Xerces])
fi

AX_LIB_XALAN([1.10.0])

if test $HAVE_XALAN != yes; then
	AC_MSG_ERROR([Cannot build without Xalan])
fi

ZIPARCHIVE

AC_ARG_WITH(udal,
 [AS_HELP_STRING([--with-udal], [udal support(default is no)])],
 [with_udal=yes], [with_udal=no])

AM_CONDITIONAL(COMP_UDAL, test $with_udal == yes)

AC_CONFIG_FILES([Makefile])

AC_OUTPUT
