CFLAGS=-g -std=c99 -D_POSIX_C_SOURCE=200112L -I../src -I../deps -pthread

corvus_test: test.c \
	test.h \
	test_parser.c \
	test_corvus.c \
	test_hash.c \
	test_slot.c \
	test_cmd.c \
	../src/hash.c ../src/slot.c ../src/parser.c ../src/socket.c \
	../src/logging.c ../src/mbuf.c ../src/command.c ../src/connection.c \
	../src/proxy.c ../src/event.c ../src/client.c ../src/server.c \
	../deps/hashmap/hash.c
	@$(CC) $(CFLAGS) $^ -o $@

test: corvus_test
	@./corvus_test

clean:
	@rm -f corvus_test
