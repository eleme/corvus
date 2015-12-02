NAME=corvus
CC=gcc -std=c99
PROFILE?=
OPTIMIZATION?=-O2
CFLAGS=-Wall $(OPTIMIZATION) $(PROFILE) -g -ggdb -D_GNU_SOURCE -I../deps
LDFLAGS=-pthread -rdynamic $(PROFILE)
BIN:=$(NAME)
SRC:=$(wildcard *.c)\
	 $(wildcard ../deps/*/*.c)
INC:=$(wildcard *.h)
OBJ:=$(SRC:%.c=%.o)
GCH:=$(INC:%.h=%.h.gch)

$(BIN): $(OBJ)

debug:
	$(MAKE) OPTIMIZATION="-O0"

profile:
	$(MAKE) OPTIMIZATION="-O0" PROFILE="-pg"

clean:
	rm -f $(BIN) $(OBJ) $(GCH)
