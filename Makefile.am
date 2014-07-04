ACLOCAL_AMFLAGS = -I ../fintp_utils/m4

bin_PROGRAMS = FinTPDBConnector	\
FinTPNODBConnector

FinTPDBConnector_SOURCES = $(COMMON_SRCS) $(DB)
FinTPDBConnector_CPPFLAGS = $(COMMON_CPPFLAGS) -I$(FINTP_HOME)/fintp_udal/src
FinTPDBConnector_LDFLAGS = $(LIBFLAGS) -L$(FINTP_HOME)/fintp_udal/.libs -lfintp_udal

FinTPNODBConnector_SOURCES = $(COMMON_SRCS)
FinTPNODBConnector_CPPFLAGS = $(COMMON_CPPFLAGS) -DNO_DB
FinTPNODBConnector_LDFLAGS = $(LIBFLAGS)

COMMON_CPPFLAGS = -DUNIX -DDEBUG_ENABLED -DXALAN_1_9	\
-I$(FINTP_HOME)/fintp_utils/src	\
-I$(FINTP_HOME)/fintp_log/src	\
-I$(FINTP_HOME)/fintp_base/src	\
-I$(FINTP_HOME)/fintp_transport/src	\
@CPPFLAGS@	\
@BOOST_CPPFLAGS@	\
@PTHREAD_CFLAGS@	\
@XERCES_CPPFLAGS@	\
@XALAN_CPPFLAGS@	\
@ZIP_CPPFLAGS@

LIBFLAGS = @LDFLAGS@ @LIBS@	\
@BOOST_LDFLAGS@ @BOOST_REGEX_LIB@ -lboost_regex\
@PTHREAD_LIBS@	\
@XERCES_LDFLAGS@ -lxerces-c	\
@XALAN_LDFLAGS@ -lxalan-c	\
-L$(FINTP_HOME)/fintp_utils/.libs	-lfintp_utils	\
-L$(FINTP_HOME)/fintp_log/.libs -lfintp_log	\
-L$(FINTP_HOME)/fintp_transport/.libs -lfintp_transport	\
-L$(FINTP_HOME)/fintp_base/.libs -lfintp_base 

AUTOMAKE_OPTIONS = foreign

COMMON_SRCS = src/FileConnector/FileFetcher.cpp	\
src/FileConnector/FilePublisher.cpp	\
src/MqConnector/MqFetcher.cpp	\
src/MqConnector/MqPublisher.cpp	\
src/Connector.cpp	\
src/ConnectorMain.cpp	\
src/Endpoint.cpp	\
src/Message.cpp	\
src/VersionInfo.cpp

DB = src/DbConnector/DbFetcher.cpp	\
src/DbConnector/DbPublisher.cpp

nobase_include_HEADERS = src/DbConnector/DbFetcher.h	\
src/DbConnector/DbPublisher.h	\
src/FileConnector/FileFetcher.h	\
src/FileConnector/FilePublisher.h	\
src/MqConnector/MqFetcher.h	\
src/MqConnector/MqPublisher.h	\
src/Connector.h	\
src/ConnectorMain.h	\
src/Endpoint.h	\
src/Message.h

SUBDIRS = .
