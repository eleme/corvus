default: test
NAME=corvus_test
CC=gcc -std=c99
CFLAGS=-g -D_GNU_SOURCE -DCORVUS_TEST -I../deps -I ../src
LDFLAGS=-pthread
BIN:=$(NAME)
SRC:=$(wildcard *.c)\
	 $(wildcard ../src/*.c)\
	 $(wildcard ../deps/*/*.c)
INC:=$(wildcard ../src/*.h)
OBJ:=$(SRC:.c=.o)
GCH:=$(INC:.h=.h.gch)

$(BIN): $(OBJ)
test: $(BIN)
	./$(BIN)
clean:
	@rm -f $(BIN) $(OBJ) $(GCH)
# vim:set noet:
