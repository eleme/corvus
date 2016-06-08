#ifndef SERVER_H
#define SERVER_H

struct connection;
struct context;

struct connection *server_create(struct context *ctx, int fd);
void server_eof(struct connection *server, const char *reason);

#endif /* end of include guard: SERVER_H */
