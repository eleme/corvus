#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <sys/eventfd.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
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
        LOG(ERROR, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

static int set_reuseport(int fd)
{
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(int)) == -1) {
        LOG(ERROR, "setsockopt SO_REUSEPORT: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

static int _listen(int fd, struct sockaddr *sa, socklen_t len, int backlog)
{
    if (bind(fd, sa, len) == -1) {
        LOG(ERROR, "bind: %s", strerror(errno));
        return CORVUS_ERR;
    }

    if (listen(fd, backlog) == -1) {
        LOG(ERROR, "listen: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

static int _accept(int fd, struct sockaddr *sa, socklen_t *len)
{
    int s = -1;
    while (1) {
        s = accept(fd, sa, len);
        if (s == -1) {
            switch (errno) {
                case EINTR: continue;
                case EAGAIN: return CORVUS_AGAIN;
            }
            LOG(ERROR, "accept: %s", strerror(errno));
            return CORVUS_ERR;
        }
        break;
    }
    return s;
}

static inline int _socket(int domain, int type, int protocol)
{
    int fd;
    if ((fd = socket(domain, type | SOCK_CLOEXEC, protocol)) == -1) {
        LOG(ERROR, "socket: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return fd;
}

static int _connect(int fd, const struct sockaddr *addr, socklen_t addrlen)
{
    while (1) {
        int status = connect(fd, addr, addrlen);
        if (status == -1) {
            switch (errno) {
                case EINTR: continue;
                case EINPROGRESS: return CORVUS_INPROGRESS;
                default:
                    LOG(ERROR, "connect: %s", strerror(errno));
                    return CORVUS_ERR;
            }
        }
        break;
    }
    return CORVUS_OK;
}

static int _getaddrinfo(const char *addr, int port, struct addrinfo **servinfo, int socktype)
{
    int err = 0;
    char _port[6];
    struct addrinfo hints;

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = socktype;

    if ((err = getaddrinfo(addr, _port, &hints, servinfo)) != 0) {
        LOG(ERROR, "getaddrinfo: %s", gai_strerror(err));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int socket_set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        LOG(WARN, "fcntl: %s", strerror(errno));
        return CORVUS_ERR;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG(WARN, "fail to set nonblock for fd %d: %s", fd, strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int socket_set_tcpnodelay(int fd)
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(int)) == -1) {
        LOG(WARN, "setsockopt TCP_NODELAY: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
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
    int s = -1;
    struct addrinfo *p, *servinfo;

    if (_getaddrinfo(bindaddr, port, &servinfo, SOCK_STREAM) == CORVUS_ERR) {
        LOG(ERROR, "socket_create_server: fail to get address info");
        return CORVUS_ERR;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((s = _socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            continue;
        }
        break;
    }

    if (p == NULL || s == -1) {
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    if (socket_set_nonblocking(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    if (set_reuseaddr(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    if (set_reuseport(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    if (_listen(s, p->ai_addr, p->ai_addrlen, 1024) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }
    freeaddrinfo(servinfo);

    return s;
}

int socket_create_stream()
{
    return _socket(AF_INET, SOCK_STREAM, 0);
}

int socket_create_udp_client()
{
    return _socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
}

int socket_accept(int fd, char *ip, size_t ip_len, int *port)
{
    int s;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    s = _accept(fd, (struct sockaddr*)&sa, &salen);
    if (s == CORVUS_AGAIN || s == CORVUS_ERR) return s;

    struct sockaddr_in *addr = (struct sockaddr_in*)&sa;
    if (ip) inet_ntop(AF_INET, (void*)&(addr->sin_addr), ip, ip_len);
    if (port) *port = ntohs(addr->sin_port);
    return s;
}

int socket_connect(int fd, char *addr, int port)
{
    int status = CORVUS_ERR;
    struct addrinfo *p, *addrs;

    if (_getaddrinfo(addr, port, &addrs, SOCK_STREAM) == CORVUS_ERR) {
        LOG(ERROR, "socket_connect: fail to get address info");
        return CORVUS_ERR;
    }

    for (p = addrs; p != NULL; p = p->ai_next) {
        status = _connect(fd, p->ai_addr, p->ai_addrlen);
        if (status == CORVUS_ERR) continue;
        break;
    }
    freeaddrinfo(addrs);

    return status;
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
        return n;
    }
    return CORVUS_ERR;
}

int socket_get_sockaddr(char *addr, int port, struct sockaddr_in *dest, int socktype)
{
    struct addrinfo *addrs;
    if (_getaddrinfo(addr, port, &addrs, socktype) == CORVUS_ERR) {
        LOG(ERROR, "socket_get_sockaddr: fail to get address info");
        return CORVUS_ERR;
    }
    if (addrs == NULL) {
        LOG(ERROR, "socket_get_sockaddr: fail to get address");
        return CORVUS_ERR;
    }
    memcpy(dest, addrs->ai_addr, addrs->ai_addrlen);
    freeaddrinfo(addrs);
    return CORVUS_OK;
}

void socket_address_init(struct address *addr, char *ip, int len, int port)
{
    int size = MIN(IP_LEN, len);
    strncpy(addr->ip, ip, size);
    if (size >= IP_LEN) {
        LOG(WARN, "hostname length exceed %d", IP_LEN);
    }
    addr->ip[size] = '\0';
    addr->port = port;
    addr->ro = 0;
}

int socket_parse_port(char *ptr, uint16_t *res)
{
    char *end;
    int port = strtol(ptr, &end, 0);
    if (*end != '\0' || port > 0xFFFF || port <= 0) return CORVUS_ERR;
    *res = port;
    return CORVUS_OK;
}

int socket_parse(char *addr, ssize_t *len)
{
    uint16_t port;
    char *colon;

    colon = strchr(addr, ':');
    if (colon == NULL) return CORVUS_ERR;

    if (socket_parse_port(colon + 1, &port) == CORVUS_ERR) {
        return CORVUS_ERR;
    }
    *len = colon - addr;
    return port;
}

int socket_parse_ip(char *addr, struct address *address)
{
    ssize_t len;
    int port = socket_parse(addr, &len);
    if (port == CORVUS_ERR) return CORVUS_ERR;

    char a[len + 1];
    memcpy(a, addr, len);
    a[len] = '\0';

    struct sockaddr_in addr_in;
    if (socket_get_sockaddr(a, port, &addr_in, SOCK_STREAM) == CORVUS_ERR) {
        return CORVUS_ERR;
    }
    if (inet_ntop(AF_INET, (void*)&(addr_in.sin_addr),
                address->ip, sizeof(address->ip)) == NULL)
    {
        LOG(ERROR, "socket_parse_ip: %s", strerror(errno));
        return CORVUS_ERR;
    }
    address->port = port;
    return CORVUS_OK;
}

int socket_parse_addr(char *addr, struct address *address)
{
    ssize_t len;
    int port = socket_parse(addr, &len);
    if (port == CORVUS_ERR) return CORVUS_ERR;
    socket_address_init(address, addr, len, port);
    return port;
}

void socket_get_key(struct address *addr, char *dst)
{
    int n = snprintf(dst, DSN_LEN, "%s:%d", addr->ip, addr->port);
    if (n >= DSN_LEN) {
        LOG(WARN, "hostname %s length exceed %d", addr->ip, IP_LEN - 1);
        dst[DSN_LEN - 1] = '\0';
    }
}

int socket_create_eventfd()
{
    int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (fd == -1) {
        LOG(WARN, "%s: %s", __func__, strerror(errno));
    }
    return fd;
}

int socket_trigger_event(int evfd)
{
    uint64_t u = 1;
    ssize_t s = write(evfd, &u, sizeof(u));
    if (s != sizeof(u)) {
        LOG(ERROR, "%s: %s", __func__, strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}
