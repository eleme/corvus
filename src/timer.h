#ifndef TIMER_H
#define TIMER_H

struct context;
struct connection;

int timer_init(struct connection *timer, struct context *ctx);
int timer_start(struct connection *timer);

#endif /* end of include guard: TIMER_H */
