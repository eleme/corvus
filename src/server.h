#ifndef __SERVER_H
#define __SERVER_H

struct connection;
struct context;

struct connection *server_create(struct context *ctx, int fd);

#endif /* end of include guard: __SERVER_H */
