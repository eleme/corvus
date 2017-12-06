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

// 设置文件描述符fd端口重用, SO_REUSEADDR用于TCP处于time_wait状态下的socket, 重复绑定使用
static int set_reuseaddr(int fd)
{
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int)) == -1) {
        LOG(ERROR, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 设置文件描述符fd允许完全重复捆绑(允许多个进程或者线程绑定到同一端口), 为了提高性能
// 这样做的好处是, 可以启动多个线程来监听并处理同一个端口上的请求, 而不是使用旧有的单
// 线程监听, 多worker执行, 消除了处理海量请求的瓶颈
static int set_reuseport(int fd)
{
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(int)) == -1) {
        LOG(ERROR, "setsockopt SO_REUSEPORT: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 通常编写socket server时的第二步, 绑定socket和地址, 以及设置socket为listen模式.
// 在创建socket之后执行
static int cv_listen(int fd, struct sockaddr *sa, socklen_t len, int backlog)
{
    // 把地址与socket文件描述符绑定
    if (bind(fd, sa, len) == -1) {
        LOG(ERROR, "bind: %s", strerror(errno));
        return CORVUS_ERR;
    }

    // 设置这个socket文件描述符开始接收连接, backlog指定了同时能处理的最大连接数
    if (listen(fd, backlog) == -1) {
        LOG(ERROR, "listen: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

static int cv_accept(int fd, struct sockaddr *sa, socklen_t *len)
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

// 编写socket server的第一步, 创建套接字
// 建立新的socket用于通信, 返回该socket的文件描述符
static inline int cv_socket(int domain, int type, int protocol)
{
    int fd;
    if ((fd = socket(domain, type | SOCK_CLOEXEC, protocol)) == -1) {
        LOG(ERROR, "socket: %s", strerror(errno));
        return CORVUS_ERR;
    }
    return fd;
}

static int cv_connect(int fd, const struct sockaddr *addr, socklen_t addrlen)
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

// 获取目标地址的信息, 返回值存放在第三个参数中
static int cv_getaddrinfo(const char *addr, int port, struct addrinfo **servinfo, int socktype)
{
    int err = 0;
    char port_str[6];
    struct addrinfo hints;

    snprintf(port_str, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;      // 期望返回的ai_family
    hints.ai_socktype = socktype;   // 期望返回的sock类型

    // getaddrinfo函数可以处理名字到地址以及服务到端口的转换, 返回一个addrinfo的指针. 该函数所需参数依次为
    // 1. 主机名
    // 2. 端口号
    // 3. 指向某个addrinfo的指针(调用者在这个对象中填入关于期望返回的信息类型的暗示)
    // 4. 本函数通过最后这个变量指针来返回一个指向addrinfo结构体链表的指针
    if ((err = getaddrinfo(addr, port_str, &hints, servinfo)) != 0) {
        LOG(ERROR, "getaddrinfo: %s", gai_strerror(err));
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

// 把文件描述符fd设置为非阻塞I/O
int socket_set_nonblocking(int fd)
{
    // 获取fd的文件状态标志
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        LOG(WARN, "fcntl: %s", strerror(errno));
        return CORVUS_ERR;
    }
    // 更新fd的文件状态标志为非阻塞I/O
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

// 创建一个socket server. 通常创建socket server 分三步:
// 1. 创建socket
// 2. bind: 绑定socket和地址, listen: 设置socket为listen状态
// 3. accept: 开始接收并处理请求
int socket_create_server(char *bindaddr, int port)
{
    int s = -1;
    struct addrinfo *p, *servinfo;

    // 获取目标地址的相关信息, 存储在servinfo中, 且返回值是个指针链表
    if (cv_getaddrinfo(bindaddr, port, &servinfo, SOCK_STREAM) == CORVUS_ERR) {
        LOG(ERROR, "socket_create_server: fail to get address info");
        return CORVUS_ERR;
    }

    // 遍历servinfo这个链表, 创建socket, 获取文件描述符(上面的第一步)
    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((s = cv_socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            continue;
        }
        break;
    }

    if (p == NULL || s == -1) {
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    // 设置非阻塞I/O
    if (socket_set_nonblocking(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    // 设置端口重用(重复利用time_wait状态的tcp socket)
    if (set_reuseaddr(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    // 设置允许多线程绑定监听同一端口
    if (set_reuseport(s) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }

    // bind和listen(把socket和地址绑定, 监听) (上面的第二步)
    if (cv_listen(s, p->ai_addr, p->ai_addrlen, 1024) == -1) {
        close(s);
        freeaddrinfo(servinfo);
        return CORVUS_ERR;
    }
    freeaddrinfo(servinfo);

    return s;
}

int socket_create_stream()
{
    return cv_socket(AF_INET, SOCK_STREAM, 0);
}

int socket_create_udp_client()
{
    return cv_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
}

int socket_accept(int fd, char *ip, size_t ip_len, int *port)
{
    int s;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    s = cv_accept(fd, (struct sockaddr*)&sa, &salen);
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

    if (cv_getaddrinfo(addr, port, &addrs, SOCK_STREAM) == CORVUS_ERR) {
        LOG(ERROR, "socket_connect: fail to get address info");
        return CORVUS_ERR;
    }

    for (p = addrs; p != NULL; p = p->ai_next) {
        status = cv_connect(fd, p->ai_addr, p->ai_addrlen);
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
    if (cv_getaddrinfo(addr, port, &addrs, socktype) == CORVUS_ERR) {
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
}

int socket_parse_port(char *ptr, uint16_t *res)
{
    char *end;
    int port = strtol(ptr, &end, 0);
    if ((*end != '\0' && *end != '@') || port > 0xFFFF || port <= 0) return CORVUS_ERR;
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
