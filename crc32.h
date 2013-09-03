#ifndef __CRC32_H_
#define __CRC32_H_
#include <stdio.h>
#include <stdint.h>

uint32_t crc32(char *s, size_t len);
uint32_t crc32_short(const char *, size_t len);

struct crc32_ctx {
	uint32_t val;
};

void crc32_init(struct crc32_ctx *);
void crc32_update(struct crc32_ctx *, const char *, size_t);
uint32_t crc32_final(struct crc32_ctx *);

#endif
