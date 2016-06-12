#ifndef PROXY_H
#define PROXY_H

struct connection;
struct context;

int proxy_init(struct connection *proxy, struct context *ctx, char *host, int port);

#endif /* end of include guard: PROXY_H */
