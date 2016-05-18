default: test

NAME=corvus_test
CC=gcc
CFLAGS=-std=c99 -g -D_GNU_SOURCE -DCORVUS_TEST -I../src -I../deps/jemalloc/include
LDFLAGS=-pthread

JEMALLOC_H=../deps/jemalloc/include/jemalloc/jemalloc.h
JEMALLOC=../deps/jemalloc/lib/libjemalloc.a
BIN:=$(NAME)
SRC:=$(wildcard *.c) $(wildcard ../src/*.c)
INC:=$(wildcard ../src/*.h)

$(BIN): $(JEMALLOC_H) $(SRC) $(INC) $(JEMALLOC)

$(JEMALLOC_H):
	-cd ../deps && $(MAKE) jemalloc

test: $(BIN)
	@./$(BIN)

clean:
	rm -f $(BIN) $(OBJ)
