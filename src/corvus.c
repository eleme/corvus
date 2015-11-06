#define _GNU_SOURCE
#include <errno.h>
#include "corvus.h"
#include "mbuf.h"
#include "slot.h"
#include "logging.h"
#include "event.h"
#include "proxy.h"

#define MIN(a, b) (a > b ? b : a)

static struct {
    uint16_t bind;
    struct node_conf node;
    int thread;
    int loglevel;
    int syslog;
} config;

static void context_init(struct context *ctx, bool syslog, int log_level)
{
    ctx->syslog = syslog;
    ctx->log_level = log_level;
    ctx->server_table = hash_new();
    mbuf_init(ctx);
    log_init(ctx);
}

int config_add(char *name, char *value)
{
    int name_len = strlen(name),
        value_len = strlen(value);
    if (name_len <= 0 || value_len <= 0) return -1;

    char *end;
    if (strncmp(name, "bind", MIN(4, name_len)) == 0) {
        config.bind = strtoul(value, &end, 0);
        if (config.bind > 0xFFFF) return -1;
    } else if (strncmp(name, "syslog", MIN(6, name_len)) == 0) {
        config.syslog = strtoul(value, &end, 0);
    } else if (strncmp(name, "thread", MIN(6, name_len)) == 0) {
        config.thread = strtoul(value, &end, 0);
        if (config.thread < 0) config.thread = 4;
    } else if (strncmp(name, "loglevel", MIN(8, name_len)) == 0) {
        if (strncmp(value, "debug", MIN(5, value_len)) == 0) {
            config.loglevel = DEBUG;
        } else if (strncmp(value, "warn", MIN(4, value_len)) == 0) {
            config.loglevel = WARN;
        } else if (strncmp(value, "error", MIN(4, value_len)) == 0) {
            config.loglevel = ERROR;
        } else {
            config.loglevel = INFO;
        }
    } else if (strncmp(name, "node", MIN(4, name_len)) == 0) {
        int i;
        if (config.node.nodes != NULL) {
            for (i = 0; i < config.node.len; i++) {
                free(config.node.nodes[i]);
            }
            free(config.node.nodes);
            config.node.nodes = NULL;
            config.node.len = 0;
        }
        int len = 0, size;
        char *p = strtok(value, ",");
        while (p) {
            size = strlen(p);
            config.node.nodes = realloc(config.node.nodes, sizeof(char*) * (++len));
            config.node.nodes[len - 1] = malloc(sizeof(char) * (size + 1));
            memcpy(config.node.nodes[len - 1], p, size);
            config.node.nodes[len - 1][size] = '\0';
            config.node.len++;
            p = strtok(NULL, ",");
        }
    }
    return 0;
}

int read_conf(const char *filename)
{
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "config file: %s", strerror(errno));
        return -1;
    }
    size_t len = 0;
    int r, i;
    char *line = NULL;
    while ((r = getline(&line, &len, fp)) != -1) {
        char name[r], value[r];
        if (r > 0 && line[0] == '#') {
            free(line);
            line = NULL;
            continue;
        }
        for (i = 0; i < r && (line[i] == ' ' || line[i] == '\r'
                    || line[i] == '\t' || line[i] == '\n'); i++);
        if (i == r) {
            free(line);
            line = NULL;
            continue;
        }

        sscanf(line, "%s%s", name, value);
        if (config_add(name, value) == -1) {
            free(line);
            fclose(fp);
            return -1;
        }
        free(line);
        line = NULL;
    }
    fclose(fp);
    return 0;
}

void main_loop()
{
    struct context ctx;
    struct event_loop *loop = event_create(1024);

    context_init(&ctx, config.syslog, config.loglevel);
    ctx.loop = loop;

    struct connection *proxy = proxy_create(&ctx, "0.0.0.0", config.bind);
    if (proxy == NULL) {
        LOG(ERROR, "Error: can not create proxy");
        return;
    }

    event_register(loop, proxy);
    while (1) {
        event_wait(loop, -1);
    }
}

int main(int argc, const char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Usage: corvus example.conf\n");
        return EXIT_FAILURE;
    }

    if (read_conf(argv[1]) == -1) {
        fprintf(stderr, "Error: invalid config\n");
        return EXIT_FAILURE;
    }
    init_command_map();
    slot_init_updater(config.syslog, config.loglevel);
    slot_create_job(SLOT_UPDATE_INIT, &config.node);

    main_loop();
    return EXIT_SUCCESS;
}
