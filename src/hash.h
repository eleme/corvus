#ifndef __HASH_H
#define __HASH_H

#include <stdint.h>
#include <stdio.h>
#include "parser.h"

uint32_t lookup3_hash(struct pos_array *pos);
uint16_t crc16(const struct pos_array *pos);

#endif /* end of include guard: __HASH_H */
