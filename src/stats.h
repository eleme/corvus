#ifndef __STATS_H
#define __STATS_H

struct stats;

int stats_init(int interval);
void stats_kill();
int stats_resolve_addr(char *addr);
void stats_get(struct stats *stats);

#endif /* end of include guard: __STATS_H */
