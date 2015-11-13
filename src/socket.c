#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include "corvus.h"
#include "socket.h"
#include "mbuf.h"
#include "logging.h"

static int set_reuseaddr(int fd)
{
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int)) == -1) {
        perror("set_reuseaddr: setsockopt");
        return -1;
    }
    return 0;
}

static int set_reuseport(int fd)
{
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(int)) == -1) {
        perror("set_reuseport: setsockopt");
        return -1;
    }
    return 0;
}

static int _listen(int fd, struct sockaddr *sa, socklen_t len, int backlog)
{
    if (bind(fd, sa, len) == -1) {
        perror("socket_listen: bind");
        return -1;
    }

    if (listen(fd, backlog) == -1) {
        perror("socket_listen: listen");
        return -1;
    }
    return 0;
}

static int _accept(int fd, struct sockaddr *sa, socklen_t *len)
{
    int s = -1;
    while (1) {
        s = accept(fd, sa, len);
        if (fd == -1) {
            if (errno == EINTR) {
                continue;
            } else {
                perror("_accept: accept");
                return -1;
            }
        }
        break;
    }
    return s;
}

static inline int _socket(int domain, int type, int protocol)
{
    return socket(domain, type | SOCK_CLOEXEC, protocol);
}

static int _connect(int fd, const struct sockaddr *addr, socklen_t addrlen)
{
    while (1) {
        int status = connect(fd, addr, addrlen);
        if (status == -1) {
            switch (errno) {
                case EINTR: continue;
                case EINPROGRESS: return CORVUS_INPROGRESS;
                default: return CORVUS_ERR;
            }
        }
        break;
    }
    return CORVUS_OK;
}

static struct addrinfo *_getaddrinfo(const char *addr, int port)
{
    int err;
    char _port[6];
    struct addrinfo hints, *servinfo;

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((err = getaddrinfo(addr, _port, &hints, &servinfo)) != 0) {
        LOG(ERROR, "get address info: %s", strerror(err));
        return NULL;
    }
    return servinfo;
}

int socket_set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int socket_set_timeout(int fd, int timeout)
{
    struct timeval tv;
    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
        LOG(ERROR, "setsockopt SO_SNDTIMEO: %s", strerror(errno));
        return CORVUS_ERR;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
        LOG(ERROR, "setsockopt SO_SNDTIMEO: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int socket_create_server(char *bindaddr, int port)
{
    int s;
    struct addrinfo *p, *servinfo;

    servinfo = _getaddrinfo(bindaddr, port);
    if (servinfo == NULL) return -1;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((s = _socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            continue;
        }
        break;
    }
    freeaddrinfo(servinfo);

    if (p == NULL) {
        perror("unable to bind");
        return -1;
    }
    if (socket_set_nonblocking(s) == -1) {
        close(s);
        return -1;
    }

    if (set_reuseaddr(s) == -1) {
        close(s);
        return -1;
    }

    if (set_reuseport(s) == -1) {
        close(s);
        return -1;
    }

    if (_listen(s, p->ai_addr, p->ai_addrlen, 1024) == -1) {
        close(s);
        return -1;
    }

    return s;
}

int socket_create_stream()
{
    int s;
    if ((s = _socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket_create_stream: socket");
        return -1;
    }
    return s;
}

int socket_accept(int fd, char *ip, size_t ip_len, int *port)
{
    int s;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    s = _accept(fd, (struct sockaddr*)&sa, &salen);
    if (s == -1) {
        if (errno == EAGAIN) {
            return errno;
        }
        return -1;
    }

    struct sockaddr_in *addr = (struct sockaddr_in*)&sa;
    if (ip) inet_ntop(AF_INET, (void*)&(addr->sin_addr), ip, ip_len);
    if (port) *port = ntohs(addr->sin_port);
    return s;
}

int socket_connect(int fd, char *addr, int port)
{
    int status;
    int retval = 0;
    struct addrinfo *p, *addrs;

    addrs = _getaddrinfo(addr, port);
    if (addrs == NULL) return -1;

    for (p = addrs; p != NULL; p = p->ai_next) {
        status = _connect(fd, p->ai_addr, p->ai_addrlen);
        if (status == CORVUS_ERR) continue;
        break;
    }
    freeaddrinfo(addrs);

    if (p == NULL) {
        retval = -1;
    }
    return retval;
}

int socket_read(int fd, struct mbuf *buf)
{
    ssize_t n;
    while (1) {
        n = read(fd, buf->last, mbuf_write_size(buf));
        if (n == -1) {
            switch (errno) {
                case EINTR: continue;
                case EAGAIN: return CORVUS_AGAIN;
                default:
                    LOG(WARN, "socket read: %s", strerror(errno));
                    return CORVUS_ERR;
            }
        }
        stats.recv_bytes += n;
        buf->last += n;
        return n;
    }
    return CORVUS_ERR;
}

int socket_write(int fd, struct iovec *iov, int invcnt)
{
    int n;
    while (1) {
        n = writev(fd, iov, invcnt);
        if (n == -1) {
            switch (errno) {
                case EINTR: continue;
                case EAGAIN: return CORVUS_AGAIN;
                default:
                    LOG(WARN, "socket write: %s", strerror(errno));
                    return CORVUS_ERR;
            }
        }
        stats.send_bytes += n;
        return n;
    }
    return CORVUS_ERR;
}

void socket_get_addr(char *host, int host_len, int port, struct address *addr)
{
    int max_len = sizeof(addr->host) / sizeof(char);

    strncpy(addr->host, host, MIN(host_len, max_len));
    if (host_len >= max_len) {
        LOG(WARN, "hostname length exceed %d", max_len - 1);
        addr->host[max_len - 1] = '\0';
    } else {
        addr->host[host_len] = '\0';
    }
    addr->port = port;
}

int socket_parse_addr(char *addr, struct address *address)
{
    unsigned long port;
    char *colon, *end;

    colon = strchr(addr, ':');
    if (colon == NULL) return CORVUS_ERR;

    port = strtoul(colon + 1, &end, 0);
    if (*end != '\0' || end == colon + 1) return CORVUS_ERR;
    if (port > 0xFFFF) return CORVUS_ERR;

    socket_get_addr(addr, colon - addr, port, address);
    return port;
}

char *socket_get_key(struct address *addr)
{
    char *dest = calloc(1, sizeof(struct address) + 8);
    sprintf(dest, "%s:%d", addr->host, addr->port);
    return dest;
}
