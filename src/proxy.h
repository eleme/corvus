#ifndef __PROXY_H
#define __PROXY_H

struct connection;
struct context;

struct connection *proxy_create(struct context *ctx, char *host, int port);

#endif /* end of include guard: __PROXY_H */
