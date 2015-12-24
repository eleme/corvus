NAME=corvus
CC=gcc
PROFILE?=
OPTIMIZATION?=-O2
CFLAGS=-std=c99 -Wall $(OPTIMIZATION) $(PROFILE) -g -ggdb -D_GNU_SOURCE
LDFLAGS=-pthread -rdynamic $(PROFILE)

BIN:=$(NAME)
SRC:=$(wildcard *.c)
OBJ:=$(SRC:%.c=%.o)

$(BIN): $(OBJ)

debug:
	$(MAKE) OPTIMIZATION="-O0"

profile:
	$(MAKE) OPTIMIZATION="-O0" PROFILE="-pg"

clean:
	rm -f $(BIN) $(OBJ)
