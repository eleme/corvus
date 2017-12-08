#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <libgen.h>
#include <inttypes.h>
#include <linux/limits.h>
#include "corvus.h"
#include "alloc.h"
#include "logging.h"
#include "socket.h"
#include "array.h"

#define DEFAULT_THREAD 4
#define DEFAULT_BUFSIZE 16384
#define MIN_BUFSIZE 64
#define TMP_CONFIG_FILE "tmp-corvus.conf"

static pthread_mutex_t lock_conf_node = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lock_config_rewrite = PTHREAD_MUTEX_INITIALIZER;
const char * CONFIG_OPTIONS[] = {
    "cluster",
    "bind",
    "node",
    "thread",
    "loglevel",
    "syslog",
    "statsd",
    "metric_interval",
    "stats",
    "read-strategy",
    "requirepass",
    "client_timeout",
    "server_timeout",
    "bufsize",
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "slowlog-statsd-enabled",
};

void config_init()
{
    memset(config.cluster, 0, CLUSTER_NAME_SIZE + 1);
    strncpy(config.cluster, "default", CLUSTER_NAME_SIZE);

    config.bind = 12345;
    config.node = cv_calloc(1, sizeof(struct node_conf));
    config.node->refcount = 1;
    config.thread = DEFAULT_THREAD;
    config.loglevel = INFO;
    config.syslog = 0;
    config.stats = false;
    config.client_timeout = 0;
    config.server_timeout = 0;
    config.bufsize = DEFAULT_BUFSIZE;
    config.requirepass = NULL;
    config.readslave = config.readmasterslave = false;
    config.slowlog_max_len = 1024;
    config.slowlog_log_slower_than = -1;
    config.slowlog_statsd_enabled = 0;

    memset(config.statsd_addr, 0, sizeof(config.statsd_addr));
    config.metric_interval = 10;
}

void config_free()
{
    config_node_dec_ref(config.node);
    pthread_mutex_destroy(&lock_conf_node);
    pthread_mutex_destroy(&lock_config_rewrite);
}

void config_boolean(bool *item, char *value)
{
    if (strcasecmp(value, "false") == 0) {
        *item = false;
    } else if (strcasecmp(value, "true") == 0) {
        *item = true;
    } else {
        if (atoi(value) == 0) {
            *item = false;
        } else {
            *item = true;
        }
    }
}

struct node_conf *config_get_node()
{
    pthread_mutex_lock(&lock_conf_node);
    struct node_conf *node = config.node;
    int refcount = ATOMIC_INC(node->refcount, 1);
    pthread_mutex_unlock(&lock_conf_node);
    assert(refcount >= 1);
    return node;
}

void config_set_node(struct node_conf *node)
{
    pthread_mutex_lock(&lock_conf_node);
    struct node_conf *oldnode = config.node;
    config.node = node;
    pthread_mutex_unlock(&lock_conf_node);
    config_node_dec_ref(oldnode);
}

void config_node_dec_ref(struct node_conf *node)
{
    int refcount = ATOMIC_DEC(node->refcount, 1);
    assert(refcount >= 0);
    if (refcount == 0) {
        cv_free(node->addr);
        cv_free(node);
    }
}

void config_node_to_str(char *str, size_t max_len)
{
    struct node_conf *nodes = config_get_node();
    char buf[ADDRESS_LEN + 1];
    for (size_t i = 0; i != nodes->len; i++) {
        struct address *addr = &nodes->addr[i];
        size_t len = snprintf(buf, max_len, "%s:%u",
            addr->ip, addr->port);
        size_t comma_len = i > 0 ? 1 : 0;
        if (len + comma_len > max_len) break;
        if (comma_len) *str++ = ',';
        strcpy(str, buf);
        str += len;
        max_len -= len + comma_len;
    }
    config_node_dec_ref(nodes);
}

int parse_int(char *s, int *result)
{
    const int max = 100000000;
    int res = 0, sign = 1;
    if (*s == '-') {
        sign = -1;
        s++;
    } else if (*s == '+') {
        s++;
    }
    for (; *s; s++) {
        int digit = *s - '0';
        if (digit < 0 || digit > 9) {
            LOG(WARN, "parse_int: invalid digit char '%c'", *s);
            return CORVUS_ERR;
        }
        res = 10 * res + digit;
        if (res > max) {
            LOG(WARN, "parse_int: result too large");
            return CORVUS_ERR;
        }
    }
    *result = sign * res;
    return CORVUS_OK;
}

int config_add(char *name, char *value)
{
    int val;
#define TRY_PARSE_INT() do {                    \
    if (CORVUS_ERR == parse_int(value, &val)) { \
        return CORVUS_ERR;                      \
    }                                           \
} while(0)                                      \

    if (strcmp(name, "cluster") == 0) {
        if (strlen(value) <= 0) return CORVUS_OK;
        strncpy(config.cluster, value, CLUSTER_NAME_SIZE);
    } else if (strcmp(name, "bind") == 0) {
        if (socket_parse_port(value, &config.bind) == CORVUS_ERR) {
            return CORVUS_ERR;
        }
    } else if (strcmp(name, "syslog") == 0) {
        config_boolean(&config.syslog, value);
    } else if (strcmp(name, "read-slave") == 0) {
        LOG(WARN, "Config `read-slave` is obsolete, use `read-strategy` instead");
        config_boolean(&config.readslave, value);
    } else if (strcmp(name, "read-master-slave") == 0) {
        LOG(WARN, "Config `read-master-slave` is obsolete, use `read-strategy` instead");
        config_boolean(&config.readmasterslave, value);
        if (config.readmasterslave) {
            config.readslave = true;
        }
    } else if (strcmp(name, "read-strategy") == 0) {
        if (strcmp(value, "read-slave-only") == 0) {
            config.readmasterslave = false;
            config.readslave = true;
        } else if (strcmp(value, "both") == 0) {
            config.readmasterslave = config.readslave = true;
        } else {
            config.readmasterslave = config.readslave = false;
        }
    } else if (strcmp(name, "thread") == 0) {
        TRY_PARSE_INT();
        if (val <= 0) {
            config.thread = DEFAULT_THREAD;
        } else {
            config.thread = val;
        }
    } else if (strcmp(name, "bufsize") == 0) {
        TRY_PARSE_INT();
        if (val <= 0) {
            config.bufsize = DEFAULT_BUFSIZE;
        } else if (val < MIN_BUFSIZE) {
            config.bufsize = MIN_BUFSIZE;
        } else {
            config.bufsize = val;
        }
    } else if (strcmp(name, "client_timeout") == 0) {
        TRY_PARSE_INT();
        config.client_timeout = val < 0 ? 0 : val;
    } else if (strcmp(name, "server_timeout") == 0) {
        TRY_PARSE_INT();
        config.server_timeout = val < 0 ? 0 : val;
    } else if (strcmp(name, "statsd") == 0) {
        strncpy(config.statsd_addr, value, ADDRESS_LEN);
        config.stats = true;
    } else if (strcmp(name, "metric_interval") == 0) {
        TRY_PARSE_INT();
        config.metric_interval = val > 0 ? val : 10;
    } else if (strcmp(name, "loglevel") == 0) {
        if (strcasecmp(value, "debug") == 0) {
            ATOMIC_SET(config.loglevel, DEBUG);
        } else if (strcasecmp(value, "warn") == 0) {
            ATOMIC_SET(config.loglevel, WARN);
        } else if (strcasecmp(value, "error") == 0) {
            ATOMIC_SET(config.loglevel, ERROR);
        } else if (strcasecmp(value, "crit") == 0) {
            ATOMIC_SET(config.loglevel, CRIT);
        } else if (strcasecmp(value, "info") == 0) {
            ATOMIC_SET(config.loglevel, INFO);
        } else {
            return CORVUS_ERR;
        }
    } else if (strcmp(name, "requirepass") == 0) {
        // Last config overwrites previous ones.
        cv_free(config.requirepass);
        config.requirepass = NULL;

        if (strlen(value) > 0) {
            config.requirepass = cv_calloc(strlen(value) + 1, sizeof(char));
            memcpy(config.requirepass, value, strlen(value));
        }
    } else if (strcmp(name, "node") == 0) {
        // strtok will modify `value` to tokenize it.
        // Copy it first in case value is a string literal
        char buf[strlen(value) + 1];
        strcpy(buf, value);

        struct address *addr = NULL;
        int addr_cnt = 0;
        char *p = strtok(buf, ",");
        while (p) {
            addr = cv_realloc(addr, sizeof(struct address) * (addr_cnt + 1));
            if (socket_parse_ip(p, &addr[addr_cnt]) == -1) {
                cv_free(addr);
                return CORVUS_ERR;
            }
            addr_cnt++;
            p = strtok(NULL, ",");
        }
        if (addr_cnt == 0) {
            LOG(WARN, "received empty node value in config set");
            return CORVUS_ERR;
        }
        struct node_conf *newnode = cv_malloc(sizeof(struct node_conf));
        newnode->addr = addr;
        newnode->len = addr_cnt;
        newnode->refcount = 1;
        config_set_node(newnode);
    } else if (strcmp(name, "slowlog-log-slower-than") == 0) {
        TRY_PARSE_INT();
        ATOMIC_SET(config.slowlog_log_slower_than, val);
    } else if (strcmp(name, "slowlog-max-len") == 0) {
        TRY_PARSE_INT();
        config.slowlog_max_len = val;
    } else if (strcmp(name, "slowlog-statsd-enabled") == 0) {
        config_boolean(&config.slowlog_statsd_enabled, value);
    }
    return CORVUS_OK;
}

int config_get(const char *name, char *value, size_t max_len)
{
#define BOOL_STR(b) ((b) ? "true" : "false")

    // ignore password here
    if (strcmp(name, "cluster") == 0) {
        strncpy(value, config.cluster, max_len);
    } else if (strcmp(name, "bind") == 0) {
        snprintf(value, max_len, "%u", config.bind);
    } else if (strcmp(name, "node") == 0) {
        config_node_to_str(value, max_len);
    } else if (strcmp(name, "thread") == 0) {
        snprintf(value, max_len, "%d", config.thread);
    } else if (strcmp(name, "loglevel") == 0) {
        strncpy(value, LOG_LEVEL_STR(ATOMIC_GET(config.loglevel)), max_len);
    } else if (strcmp(name, "syslog") == 0) {
        strncpy(value, BOOL_STR(config.syslog), max_len);
    } else if (strcmp(name, "statsd") == 0) {
        strncpy(value, config.statsd_addr, max_len);
    } else if (strcmp(name, "metric_interval") == 0) {
        snprintf(value, max_len, "%d", config.metric_interval);
    } else if (strcmp(name, "stats") == 0) {
        strncpy(value, BOOL_STR(config.stats), max_len);
    } else if (strcmp(name, "read-strategy") == 0) {
        if (config.readslave && config.readmasterslave) {
            strncpy(value, "both", max_len);
        } else if (config.readslave && !config.readmasterslave) {
            strncpy(value, "read-slave-only", max_len);
        } else {
            strncpy(value, "master", max_len);
        }
    } else if (strcmp(name, "requirepass") == 0) {
        if (config.requirepass) {
            strncpy(value, config.requirepass, max_len);
        } else {
            strncpy(value, "", max_len);
        }
    } else if (strcmp(name, "client_timeout") == 0) {
        snprintf(value, max_len, "%" PRId64, config.client_timeout);
    } else if (strcmp(name, "server_timeout") == 0) {
        snprintf(value, max_len, "%" PRId64, config.server_timeout);
    } else if (strcmp(name, "bufsize") == 0) {
        snprintf(value, max_len, "%d", config.bufsize);
    } else if (strcmp(name, "slowlog-log-slower-than") == 0) {
        snprintf(value, max_len, "%d", ATOMIC_GET(config.slowlog_log_slower_than));
    } else if (strcmp(name, "slowlog-max-len") == 0) {
        snprintf(value, max_len, "%d", config.slowlog_max_len);
    } else if (strcmp(name, "slowlog-statsd-enabled") == 0) {
        strncpy(value, BOOL_STR(config.slowlog_statsd_enabled), max_len);
    } else {
        return CORVUS_ERR;
    }
    return CORVUS_OK;
}

int read_line(struct cvstr *line, FILE *fp)
{
    /* Consider the following cases:
    (1) EOF
    (2) empty line
    (3) real line length < line->capacity, after fgets '\n' exists in the line
    (4) real line length >= line->capacity, '\n' does not exist in the line
    */
    size_t len = 0;
    while (fgets(line->data + len, line->capacity - len, fp) != NULL) {
        len = strlen(line->data);
        if (line->data[len - 1] == '\n') {
            return len;
        }
        cvstr_reserve(line, line->capacity * 2);
    }
    return -1;
}

// Return whether this line contains option
// 判断读取的行的内容, 是否有配置项被启用, 并且把配置的kv写入name和value中
bool parse_option(const char *line, char *name, char *value)
{
    int i = 0;
    // 找到行开头
    for (i = 0; (line[i] == ' ' || line[i] == '\r'
            || line[i] == '\t' || line[i] == '\n'); i++);
    // 本行没有内容或者是注释, 返回false, 不用设置name和value
    if (line[i] == '\0' || line[i] == '#') {
        return false;
    }
    // 本行有有效的配置, 把本行内容按格式(xxxx aaaa)写入到name和value中
    if (2 != sscanf(line + i, "%s%s", name, value)) {
        LOG(WARN, "Ignored invalid line: %s", line);
        return false;
    }
    return true;
}

// Caller should free the returned string
char *get_abs_dir(const char *path)
{
    // `realpath` requires path exist, we should convert it to directory first.
    // `dirname` may modify the input parameter so we should copy it first.
    char *dir_buf = cv_strndup(path, strlen(path));
    const char *dir = dirname(dir_buf);

    // Here's the flaw of realpath - it doesn't provide a max_size argument.
    // The realpath will just assume the buffer is PATH_MAX long
    // and tell it to getcwd.
    // And the much more troubling stuff is the getcwd call in realpath will write
    // a '\0' at the end of the whole buffer first, which will cause corruption
    // if the buffer is not allocated properly.
    // So we MUST make it big enough.
    char *buf = cv_malloc(PATH_MAX);
    if (realpath(dir, buf) == NULL) {
        LOG(ERROR, "Fail to generate realpath: %s", strerror(errno));
        cv_free(dir_buf);
        cv_free(buf);
        return NULL;
    }
    cv_free(dir_buf);
    // returned path does not contain trailing '/'
    return buf;
}

int config_set_file_path(const char *path)
{
    char *dirname = get_abs_dir(path);
    if (dirname == NULL) {
        return CORVUS_ERR;
    }
    char buf[CONFIG_FILE_PATH_SIZE + 1];
    strncpy(buf, path, CONFIG_FILE_PATH_SIZE);
    char *filename = basename(buf);
    snprintf(config.config_file_path, CONFIG_FILE_PATH_SIZE, "%s/%s",
        dirname, filename);
    cv_free(dirname);
    return CORVUS_OK;
}

// 读取配置文件并进行配置
int config_read(const char *filename)
{
    if (strlen(filename) > CONFIG_FILE_PATH_SIZE) {
        fprintf(stderr, "Config file path is too long. Max length is %d.",
            CONFIG_FILE_PATH_SIZE);
        return CORVUS_ERR;
    }

    if (config_set_file_path(filename) == CORVUS_ERR) {
        return CORVUS_ERR;
    }

    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        // Maybe config.syslog hasn't been initialized yet
        // so should not use LOG() here
        fprintf(stderr, "config file: %s\n", strerror(errno));
        return -1;
    }
    int len = 0;
    struct cvstr line = cvstr_new(1024);
    // 打开配置文件, 并循环读取, 直至文件读取完毕
    while ((len = read_line(&line, fp)) != -1) {
        char name[len + 1], value[len + 1];
        memset(name, 0, sizeof(name));
        memset(value, 0, sizeof(value));
        // 读取本行配置文件, 把配置写入name和value中
        if (!parse_option(line.data, name, value)) {
            continue;
        }

        // 更新配置项到config对象中
        if (config_add(name, value) == -1) {
            cvstr_free(&line);
            fclose(fp);
            return CORVUS_ERR;
        }
    }
    // 关闭相关文件描述符
    cvstr_free(&line);
    fclose(fp);
    return CORVUS_OK;
}

// Caller should free the returned vector.
// Return an empty line array if fail to open or read old config file.
struct vector get_curr_file_content()
{
    struct vector lines = vector_new();
    FILE *fp = fopen(config.config_file_path, "r");
    if (fp == NULL) {
        LOG(ERROR, "Can't open config file: %s\n", strerror(errno));
        return lines;
    }

    struct cvstr line = cvstr_new(128);
    size_t len;
    while ((len = read_line(&line, fp)) != -1) {
        vector_push_back(&lines, cvstr_move(&line));
        line = cvstr_new(128);
    }
    cvstr_free(&line);
    fclose(fp);
    return lines;
}

int gen_tmp_conf(const char *tmpfile)
{
    FILE *fp = fopen(tmpfile, "w");
    if (fp == NULL) {
        fclose(fp);
        LOG(ERROR, "can't create tmp file: %s\n", strerror(errno));
        return CORVUS_ERR;
    }

    int result = CORVUS_ERR;
    // Create a brandnew config file if `lines` is empty
    struct vector lines = get_curr_file_content();

    const size_t OPTIONS_NUM = sizeof(CONFIG_OPTIONS) / sizeof(char*);
    void *WRITTEN_TAG = (void*)1;
    struct dict written_tags, options_map;
    dict_init(&written_tags);
    dict_init(&options_map);
    for (size_t i = 0; i != OPTIONS_NUM; i++) {
        dict_set(&options_map, CONFIG_OPTIONS[i], (void*)CONFIG_OPTIONS[i]);
    }
    // Since we use goto to handle error,
    // we can't use the variable length array at the same time
    struct cvstr buf = cvstr_new(1024 * 2);
    struct cvstr name = cvstr_new(1024);
    struct cvstr value = cvstr_new(1024);

    for (size_t i = 0; i != lines.size; i++) {
        const char *line = vector_at(&lines, i);
        const size_t len = strlen(line);
        cvstr_reserve(&name, len + 1);
        cvstr_reserve(&value, len + 1);
        cvstr_reserve(&buf, 2 * len + 2);  // "name value\n"
        if (!parse_option(line, name.data, value.data)
                || config_get(name.data, value.data, value.capacity) == CORVUS_ERR) {
            // Just keep other lines
            if (EOF == fputs(line, fp)) {
                goto end;
            }
            continue;
        }
        sprintf(buf.data, "%s %s\n", name.data, value.data);
        if (EOF == fputs(buf.data, fp)) {
            goto end;
        }
        // Note that dict_set will save the key in its inner space
        // and name.data will be changed to other string in next loop.
        const char *key = dict_get(&options_map, name.data);
        dict_set(&written_tags, key, WRITTEN_TAG);
    }

    // Append the options that did not exist in the config file
    for (size_t i = 0; i != OPTIONS_NUM; i++) {
        if (dict_get(&written_tags, CONFIG_OPTIONS[i]) != WRITTEN_TAG) {
            while (true) {
                int res = config_get(CONFIG_OPTIONS[i], value.data, value.capacity);
                assert(res == CORVUS_OK);
                if (!cvstr_full(&value)) break;
                cvstr_reserve(&value, value.capacity * 2);
            }
            if (strlen(value.data) == 0) {
                LOG(WARN, "Ignored empty option `%s` when rewriting config file", CONFIG_OPTIONS[i]);
                continue;
            }
            cvstr_reserve(&buf, strlen(CONFIG_OPTIONS[i]) + strlen(value.data) + 2);
            sprintf(buf.data, "%s %s\n", CONFIG_OPTIONS[i], value.data);
            if (EOF == fputs(buf.data, fp)) {
                goto end;
            }
        }
    }

    result = CORVUS_OK;

end:
    if (result == CORVUS_ERR) {
        LOG(ERROR, "can't write data to tmp file: %s\n", strerror(errno));
    }
    cvstr_free(&buf);
    cvstr_free(&name);
    cvstr_free(&value);
    dict_free(&options_map);
    dict_free(&written_tags);
    vector_free_all(&lines);
    fclose(fp);
    return result;
}

int config_rewrite()
{
    if (pthread_mutex_trylock(&lock_config_rewrite) == EBUSY) {
        return CORVUS_AGAIN;
    }

    char tmpfile[CONFIG_FILE_PATH_SIZE + 1];
    char *dirname = get_abs_dir(config.config_file_path);
    if (dirname == NULL) {
        pthread_mutex_unlock(&lock_config_rewrite);
        return CORVUS_ERR;
    }
    snprintf(tmpfile, CONFIG_FILE_PATH_SIZE, "%s/%s",
        dirname, TMP_CONFIG_FILE);
    cv_free(dirname);

    if (CORVUS_ERR == gen_tmp_conf(tmpfile)) {
        pthread_mutex_unlock(&lock_config_rewrite);
        return CORVUS_ERR;
    }

    // Atomically replace the old config file
    if (-1 == rename(tmpfile, config.config_file_path)) {
        pthread_mutex_unlock(&lock_config_rewrite);
        return CORVUS_ERR;
    }

    pthread_mutex_unlock(&lock_config_rewrite);
    LOG(INFO, "Rewrite config file successfully in %s",
        config.config_file_path);
    return CORVUS_OK;
}

bool config_option_changable(const char *option)
{
    const char *CHANGABLE_OPTIONS[] = {"node", "loglevel", "slowlog-log-slower-than"};
    const size_t OPTIONS_NUM = sizeof(CHANGABLE_OPTIONS) / sizeof(char*);
    for (size_t i = 0; i != OPTIONS_NUM; i++) {
        if (strcasecmp(CHANGABLE_OPTIONS[i], option) == 0) {
            return true;
        }
    }
    return false;
}
