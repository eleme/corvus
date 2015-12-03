default: test

NAME=corvus_test
CC=gcc
CFLAGS=-std=c99 -pedantic -g -D_GNU_SOURCE -DCORVUS_TEST -I../src
LDFLAGS=-pthread

BIN:=$(NAME)
SRC:=$(wildcard *.c) $(wildcard ../src/*.c)
INC:=$(wildcard ../src/*.h)

$(BIN): $(SRC) $(INC)

test: $(BIN)
	@./$(BIN)

clean:
	rm -f $(BIN) $(OBJ)
