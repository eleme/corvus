#ifndef __PROXY_H
#define __PROXY_H

struct connection;
struct context;

int proxy_init(struct connection *proxy, struct context *ctx, char *host, int port);

#endif /* end of include guard: __PROXY_H */
