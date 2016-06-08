#ifndef HASH_H
#define HASH_H

#include <stdint.h>
#include "parser.h"

uint32_t lookup3_hash(const char *key);
uint16_t crc16(const struct pos_array *pos);

#endif /* end of include guard: HASH_H */
