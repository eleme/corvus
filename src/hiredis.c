#include <stdlib.h>
#include "hiredis.h"

static void *create_array(const redisReadTask *task, int size)
{
    struct array_task *array = (struct array_task*)malloc(sizeof(struct array_task));
    array->size = size;
    array->item_len = 0;
    array->items = (struct str*)malloc(sizeof(struct str) * 1024);
    return array;
}

static void *create_string(const redisReadTask *task, char *str, size_t size)
{
    if (task->parent != NULL) {
        struct array_task *parent = task->parent->obj;
        struct str *s = &parent->items[parent->item_len++];
        s->str = str;
        s->len = size;
    }
    return (void*)REDIS_REPLY_STRING;
}

void hiredis_free_task(struct array_task *task)
{
    free(task->items);
    free(task);
}

int hiredis_feed_data(redisReader *r, uint8_t *buf, uint32_t len)
{
    r->buf = (char*)buf;
    r->len = len;
    r->pos = 0;
    return REDIS_OK;
}

int hiredis_get_result(redisReader *r, void **result)
{
    return redisReaderGetReply(r, result);
}

redisReader *hiredis_init()
{
    redisReplyObjectFunctions *fn;
    fn = (redisReplyObjectFunctions*)malloc(sizeof(redisReplyObjectFunctions));
    fn->createString = &create_string;
    fn->createArray = &create_array;
    fn->createInteger = NULL;
    fn->createNil = NULL;
    fn->freeObject = NULL;

    return redisReaderCreateWithFunctions(fn);
}
