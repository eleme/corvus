CFLAGS=-Wall

OBJ = event.o   \
	  mbuf.o    \
	  socket.o  \
	  proxy.o   \
	  client.o  \
	  logging.o \
	  command.o \
	  parser.o  \
	  hash.o    \
	  corvus.o

SRC = $(OBJ:%.o=%.c)

corvus: $(OBJ)

%.o: $(SRC)
