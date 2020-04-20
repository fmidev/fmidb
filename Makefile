LIB = fmidb

MAINFLAGS = -Wall -W -Wno-unused-parameter

EXTRAFLAGS = -Wpointer-arith \
	-Wcast-qual \
	-Wcast-align \
	-Wwrite-strings \
	-Wconversion \
	-Winline \
	-Wnon-virtual-dtor \
	-Wno-pmf-conversions \
	-Wsign-promo \
	-Wchar-subscripts \
	-Wold-style-cast

DIFFICULTFLAGS = -pedantic -Weffc++ -Wredundant-decls -Wshadow -Woverloaded-virtual -Wunreachable-code -Wctor-dtor-privacy

CC = /usr/bin/g++

# Default compiler flags

CFLAGS = -fPIC -std=c++11 -DPQXX_HIDE_EXP_OPTIONAL -DUNIX -O2 -g -DNDEBUG $(MAINFLAGS)
LDFLAGS = -shared 

# Special modes

CFLAGS_DEBUG = -fPIC -std=c++11 -DPQXX_HIDE_EXP_OPTIONAL -DUNIX -O0 -g -DDEBUG $(MAINFLAGS) $(EXTRAFLAGS)

LDFLAGS_DEBUG =  -shared

INCLUDES = -I$(includedir) \
           -I/usr/include/boost169 \
           -I/usr/include/oracle \
           -I/usr/include/oracle/11.2/client64 \
	   -iquote/usr/include/pqxx

LIBS =  -L$(LIBDIR) \
        -L$(LIBDIR)/odbc \
        -L/lib64 \
        -L/usr/lib64/oracle \
        -lclntsh \
        -lnsl -ldl -lm \
        -lodbc
#	/usr/lib64/libboost_date_time.a \
#        /usr/lib64/libboost_system.a

# Common library compiling template

# Installation directories

processor := $(shell uname -p)

ifeq ($(origin PREFIX), undefined)
  PREFIX = /usr
else
  PREFIX = $(PREFIX)
endif

ifeq ($(processor), x86_64)
  LIBDIR = $(PREFIX)/lib64
else
  LIBDIR = $(PREFIX)/lib
endif

objdir = obj
LIBDIR = lib

includedir = $(PREFIX)/include

ifeq ($(origin BINDIR), undefined)
  bindir = $(PREFIX)/bin
else
  bindir = $(BINDIR)
endif

# rpm variables

CWP = $(shell pwd)
BIN = $(shell basename $(CWP))

# Special modes

ifneq (,$(findstring debug,$(MAKECMDGOALS)))
  CFLAGS = $(CFLAGS_DEBUG)
  LDFLAGS = $(LDFLAGS_DEBUG)
endif

# Compilation directories

vpath %.cpp source
vpath %.h include
vpath %.o $(objdir)

# How to install

INSTALL_PROG = install -m 755
INSTALL_DATA = install -m 644

# The files to be compiled

SRCS = $(patsubst source/%,%,$(wildcard *.cpp source/*.cpp))
HDRS = $(patsubst include/%,%,$(wildcard *.h include/*.h))
OBJS = $(SRCS:%.cpp=%.o)

OBJFILES = $(OBJS:%.o=obj/%.o)

MAINSRCS = $(PROG:%=%.cpp)
SUBSRCS = $(filter-out $(MAINSRCS),$(SRCS))
SUBOBJS = $(SUBSRCS:%.cpp=%.o)
SUBOBJFILES = $(SUBOBJS:%.o=obj/%.o)

INCLUDES := -Iinclude $(INCLUDES)

# For make depend:

ALLSRCS = $(wildcard *.cpp source/*.cpp)

.PHONY: test rpm

rpmsourcedir = /tmp/$(shell whoami)/rpmbuild

# The rules

all: objdir $(LIB)
debug: objdir $(LIB)
release: objdir $(LIB)

$(LIB): $(OBJS)
	ar rcs $(LIBDIR)/libfmidb.a $(OBJFILES)
	$(CC) -o $(LIBDIR)/libfmidb.so $(LDFLAGS) $(OBJFILES)

clean:
	rm -f $(PROG) $(OBJFILES) $(LIBDIR)/$(LIB).* *~ source/*~ include/*~

install:
	mkdir -p $(libdir)
	mkdir -p $(includedir)
	@list=`cd include && ls -1 *.h`; \
	for hdr in $$list; do \
	  $(INSTALL_DATA) include/$$hdr $(includedir)/$$hdr; \
	done

	 $(INSTALL_PROG) lib/* $(libdir)

objdir:
	@mkdir -p $(objdir)
	@mkdir -p $(LIBDIR)

rpm:    clean $(LIB).spec
	rm -f $(rpmsourcedir)/lib$(LIB).tar.gz
	mkdir -p $(rpmsourcedir)
	tar -C .. --exclude .svn -czf $(rpmsourcedir)/lib$(LIB).tar.gz $(LIB)
	rpmbuild -ta $(rpmsourcedir)/lib$(LIB).tar.gz
	rm -f $(rpmsourcedir)/lib$(LIB).tar.gz

.SUFFIXES: $(SUFFIXES) .cpp

.cpp.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $(objdir)/$@ $<

-include Dependencies
