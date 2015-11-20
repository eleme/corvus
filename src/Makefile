BUILD=../build
OPTIMIZATION?=-O2
CFLAGS=-Wall $(OPTIMIZATION) -g -ggdb -std=c99 -D_GNU_SOURCE -I../deps
LDFLAGS=-pthread

SRC = client.c               \
	  command.c              \
	  connection.c           \
	  corvus.c               \
	  event.c                \
	  hash.c                 \
	  logging.c              \
	  mbuf.c                 \
	  parser.c               \
	  proxy.c                \
	  server.c               \
	  slot.c                 \
	  socket.c               \

OBJ = $(SRC:%.c=$(BUILD)/%.o)

corvus: $(OBJ) $(BUILD)/hashmap.o
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD)/hashmap.o: ../deps/hashmap/hash.c
	$(CC) $(CFLAGS) -c $^ -o $@

$(OBJ): $(BUILD)/%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

debug:
	$(MAKE) OPTIMIZATION="-O0"
