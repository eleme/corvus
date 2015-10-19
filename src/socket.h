#ifndef __SOCKET_H
#define __SOCKET_H

#include "mbuf.h"

int socket_accept(int fd, char *ip, size_t ip_len, int *port);
int socket_create_server(char *bindaddr, int port);
int socket_create_stream();
int socket_connect(int fd, char *addr, int port);
int socket_read(int fd, struct mbuf *buf);

#endif /* end of include guard: __SOCKET_H */
