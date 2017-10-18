#ifndef MD5_H
#define MD5_H

typedef struct {
    uint64_t  bytes;
    uint32_t  a, b, c, d;
    u_char    buffer[64];
} md5_t;


void md5_init(md5_t *ctx);
void md5_update(md5_t *ctx, const void *data, size_t size);
void md5_final(u_char result[16], md5_t *ctx);


static const u_char *md5_body(md5_t *ctx, const u_char *data, size_t size);

#endif
