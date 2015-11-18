#ifndef __CLIENT_H
#define __CLIENT_H

struct connection;
struct context;

struct connection *client_create(struct context *ctx, int fd);
void client_eof(struct connection *client);

#endif /* end of include guard: __CLIENT_H */
