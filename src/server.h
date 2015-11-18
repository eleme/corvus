#ifndef __SERVER_H
#define __SERVER_H

struct connection;
struct context;

struct connection *server_create(struct context *ctx, int fd);
void server_eof(struct connection *server);

#endif /* end of include guard: __SERVER_H */
