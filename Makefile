#contrib/orc_fdw/Makefile

CCC=g++
MODULE_big = orc_fdw

EXTENSION = orc_fdw
DATA = orc_fdw--1.0.1.sql

SHLIB_LINK = -L. -lorcLibBridge -L  orcLib   -lorc -lgmock  -lsnappy -lz -lprotobuf -lm -lstdc++
OBJS = orc_fdw.o

PG_CPPFLAGS = -std=c++11 -fPIC  -I.  -I orcInclude
REGRESS = orc_fdw

EXTRA_CLEAN = orc_fdw.o

ifdef USE_PGXS
#PG_CONFIG = pg_config
PG_CONFIG=/usr/pgsql-9.4/bin/pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/orc_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif