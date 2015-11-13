#include <sys/types.h>
#include <unistd.h>
#include "stats.h"

void stats_init()
{
    stats.pid = getpid();
}
