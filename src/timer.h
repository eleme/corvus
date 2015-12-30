#ifndef __TIMER_H
#define __TIMER_H

struct context;
struct connection;

int timer_init(struct connection *timer, struct context *ctx);
int timer_start(struct connection *timer);

#endif /* end of include guard: __TIMER_H */
