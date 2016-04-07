#ifndef __CLIENT_H
#define __CLIENT_H

struct connection;
struct context;
struct command;

struct connection *client_create(struct context *ctx, int fd);
void client_eof(struct connection *client);
void client_range_clear(struct connection *client, struct command *cmd);

#endif /* end of include guard: __CLIENT_H */
