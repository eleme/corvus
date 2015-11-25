#ifndef __SOCKET_H
#define __SOCKET_H

#include <arpa/inet.h>
#include <limits.h>
#include "mbuf.h"

#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 255
#endif

struct iovec;

struct address {
    char host[HOST_NAME_MAX + 1];
    uint16_t port;
};

int socket_accept(int fd, char *ip, size_t ip_len, int *port);
int socket_create_server(char *bindaddr, int port);
int socket_create_stream();
int socket_create_udp_client();
int socket_connect(int fd, char *addr, int port);
int socket_read(int fd, struct mbuf *buf);
int socket_write(int fd, struct iovec *iov, int invcnt);
int socket_get_sockaddr(char *addr, int port, struct sockaddr_in *dest, int socktype);
void socket_get_addr(char *host, int host_len, int port, struct address *addr);
int socket_set_nonblocking(int fd);
int socket_set_timeout(int fd, int timeout);
int socket_parse_addr(char *addr, struct address *address);
char *socket_get_key(struct address *addr);

#endif /* end of include guard: __SOCKET_H */
