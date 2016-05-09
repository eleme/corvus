NAME=corvus
CC=gcc
PROFILE?=
OPTIMIZATION?=-O2
CFLAGS=-std=c99 -Wall $(OPTIMIZATION) $(PROFILE) -g -ggdb -D_GNU_SOURCE -I../deps/jemalloc/include
LDFLAGS=-pthread -rdynamic $(PROFILE)

JEMALLOC_H=../deps/jemalloc/include/jemalloc/jemalloc.h
JEMALLOC=../deps/jemalloc/lib/libjemalloc.a
BIN:=$(NAME)
SRC:=$(wildcard *.c)
OBJ:=$(SRC:%.c=%.o)

$(BIN): $(JEMALLOC_H) $(OBJ) $(JEMALLOC)

$(JEMALLOC_H):
	-cd ../deps && $(MAKE) jemalloc

debug:
	$(MAKE) OPTIMIZATION="-O0"

profile:
	$(MAKE) OPTIMIZATION="-O0" PROFILE="-pg"

clean:
	rm -f $(BIN) $(OBJ)

distclean:
	-(cd ../deps && $(MAKE) distclean)
