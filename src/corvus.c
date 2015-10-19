#include <stdlib.h>
#include <string.h>

#include "hiredis.h"
#include "mbuf.h"
#include "connection.h"
#include "socket.h"
#include "event.h"
#include "corvus.h"
#include "proxy.h"
#include "logging.h"

void print_command(struct array_task *task)
{
    if (task == NULL) {
        printf("Partial or empty buffer");
        return;
    }

    int i;

    printf("Array size: %d\n", task->size);
    printf("Current Items: %d\n", task->item_len);

    for (i = 0; i < task->item_len; i++) {
        printf("%.*s\n", task->items[i].len, task->items[i].str);
    }
    printf("\n");

    hiredis_free_task(task);
}

void test_hiredis()
{
    /* char data1[] = "*2\r\n$3\r\nGET\r\n$5\r\nworld\r\n"; */
    /* char data2[] = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$3\r\n123\r\n"; */
    /* char data[] = "*5\r\n$4\r\nMSET\r\n$5\r\nhello\r\n$3\r\n123\r\n$5\r\nworld\r\n$2\r\n90\r\n"; */
    char data3[] = "*2\r\n$3\r\nGET\r\n$5\r\nworld\r\n*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$";

    struct context ctx;
    mbuf_init(&ctx);

    struct mbuf *buf = mbuf_get(&ctx);
    memcpy(buf->last, data3, sizeof(data3) - 1);
    buf->last = buf->last + sizeof(data3) - 1;

    struct array_task *result;

    redisReader *reader = hiredis_init();
    hiredis_feed_data(reader, buf->pos, mbuf_read_size(buf));

    do {
        hiredis_get_result(reader, (void**)&result);
        print_command(result);
    } while (result != NULL);

}

void test_socket(struct context *ctx)
{
    struct event_loop *loop = event_create(1024);
    struct connection *proxy = proxy_create(ctx, "localhost", 12345);
    if (proxy == NULL) {
        logger(ERROR, "can not create proxy");
        return;
    }

    event_register(loop, proxy);

    while (1) {
        event_wait(loop, -1);
    }
}

int main()
{
    struct context ctx = {.syslog = false, .log_level = DEBUG};
    mbuf_init(&ctx);
    log_init(&ctx);

    test_socket(&ctx);
    return 0;
}
