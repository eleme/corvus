NAME=corvus
CC=gcc
PROFILE?=
OPTIMIZATION?=-O2
CFLAGS=-std=c99 -Wall $(OPTIMIZATION) $(PROFILE) -g -ggdb -D_GNU_SOURCE -I../deps/jemalloc/include
LDFLAGS=-pthread -rdynamic $(PROFILE)

JEMALLOC=../deps/jemalloc/lib/libjemalloc.a
BIN:=$(NAME)
SRC:=$(wildcard *.c)
OBJ:=$(SRC:%.c=%.o)

%.o: %.c $(JEMALLOC)
	$(CC) $(CFLAGS) -c $<

$(BIN): $(OBJ) $(JEMALLOC)

$(JEMALLOC):
	$(MAKE) jemalloc -C ../deps

debug:
	$(MAKE) OPTIMIZATION="-O0"

profile:
	$(MAKE) OPTIMIZATION="-O0" PROFILE="-pg"

clean:
	rm -f $(BIN) $(OBJ)

distclean:
	$(MAKE) distclean -C ../deps
