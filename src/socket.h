#ifndef __SOCKET_H
#define __SOCKET_H

#include "mbuf.h"

struct iovec;
struct sockaddr;

int socket_accept(int fd, char *ip, size_t ip_len, int *port);
int socket_create_server(char *bindaddr, int port);
int socket_create_stream();
int socket_connect(int fd, char *addr, int port);
int socket_connect_addr(int fd, struct sockaddr *addr);
int socket_read(int fd, struct mbuf *buf);
int socket_write(int fd, struct iovec *iov, int invcnt);
int socket_get_addr(char *host, int port, struct sockaddr *addr);
int socket_addr_cmp(struct sockaddr *addr1, struct sockaddr *addr2);
int socket_set_nonblocking(int fd);
int socket_parse_addr(const char *addr, char **dest);
void socket_get_key(struct sockaddr *addr, char *dest);

#endif /* end of include guard: __SOCKET_H */
