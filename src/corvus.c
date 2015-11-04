#include "corvus.h"
#include "mbuf.h"
#include "logging.h"

void context_init(struct context *ctx, bool syslog, int log_level)
{
    ctx->syslog = syslog;
    ctx->log_level = log_level;
    mbuf_init(ctx);
    log_init(ctx);
}

int main()
{
    printf("Corvus!");
}
