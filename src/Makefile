CFLAGS=-Wall
LDFLAGS=-ldeps -L../deps

OBJ = hiredis.o \
	  event.o \
	  mbuf.o \
	  socket.o \
	  proxy.o \
	  client.o \
	  corvus.o \
	  logging.o

SRC = $(OBJ:%.o=%.c)

corvus: $(OBJ) ../deps/libdeps.a

%.o: $(SRC)
