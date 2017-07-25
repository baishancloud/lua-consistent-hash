#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <lua.h>
#include <lauxlib.h>


#define LIB_VERSION              "lua-consistent-hash-module 0.1"
#define LUA_CONSIS_HASH_TYPENAME "lua_consis_hash"

#define ERROR_NO_MEMORY     1
#define ERROR_INVALID       2
#define ERROR_INCOMPLETE    3
#define ERROR_UNKNOWN       4
#define ERROR_FINALIZED     5


/* #define N_NODE_PER_NAME 40 */
/* #define N_NODE_PER_NAME 160  */
#define N_NODE_PER_NAME 320
/* #define N_BUCKETS 1024 */
#define N_BUCKETS ( 1024*16 )
#define MAX_NAME_LEN 128
#define MAX_NODE_HASH_SRC_LEN 160
#define MAX_REPLICA 16



#define WHERESTR  "[%s, %d]: "
#define WHEREARG  __FILE__, __LINE__

#define dd( fmt, ... )
#define dinfo( fmt, ... )
#define derr( fmt, ... )
/* #define dd( fmt, ... )      fprintf( stdout, "DEBUG " WHERESTR fmt "\n", WHEREARG, ##__VA_ARGS__ ) */
/* #define dinfo( fmt, ... )   fprintf( stdout, " INFO " WHERESTR fmt "\n", WHEREARG, ##__VA_ARGS__ ) */
/* #define derr( fmt, ... )    fprintf( stderr, "ERROR " WHERESTR fmt "\n", WHEREARG, ##__VA_ARGS__ ) */

#define memzero(buf, n)       (void) memset(buf, 0, n)

#define l_table_set_const(L, c) {   \
    lua_pushliteral(L, #c);         \
    lua_pushnumber(L, c);           \
    lua_settable(L, -3);            \
}

#if LUA_VERSION_NUM < 502
#   define luaL_newlib(L, f)  { lua_newtable(L); luaL_register(L, NULL, f); }
#   define lua_rawlen(L, i)   lua_objlen(L, i)
#endif


typedef struct {
    size_t  len;
    u_char *data;
    size_t  used;
} str_t;


typedef struct {
    int      name_idx;
    uint32_t point;
} hashnode_t;


typedef struct {
    size_t      n;
    str_t      *names;
    char       *namebuf;

    size_t      n_nodes;
    hashnode_t *nodes;

    size_t      n_buckets;
    size_t     *buckets;
} continuum_t;


typedef struct {
    uint64_t  bytes;
    uint32_t  a, b, c, d;
    u_char    buffer[64];
} md5_t;


void md5_init(md5_t *ctx);
void md5_update(md5_t *ctx, const void *data, size_t size);
void md5_final(u_char result[16], md5_t *ctx);


static const u_char *md5_body(md5_t *ctx, const u_char *data, size_t size);


void
md5_init(md5_t *ctx)
{
    ctx->a = 0x67452301;
    ctx->b = 0xefcdab89;
    ctx->c = 0x98badcfe;
    ctx->d = 0x10325476;

    ctx->bytes = 0;
}


void
md5_update(md5_t *ctx, const void *data, size_t size)
{
    size_t  used, free;

    used = (size_t) (ctx->bytes & 0x3f);
    ctx->bytes += size;

    if (used) {
        free = 64 - used;

        if (size < free) {
            memcpy(&ctx->buffer[used], data, size);
            return;
        }

        memcpy(&ctx->buffer[used], data, free);
        data = (u_char *) data + free;
        size -= free;
        (void) md5_body(ctx, ctx->buffer, 64);
    }

    if (size >= 64) {
        data = md5_body(ctx, data, size & ~(size_t) 0x3f);
        size &= 0x3f;
    }

    memcpy(ctx->buffer, data, size);
}


void
md5_final(u_char result[16], md5_t *ctx)
{
    size_t  used, free;

    used = (size_t) (ctx->bytes & 0x3f);

    ctx->buffer[used++] = 0x80;

    free = 64 - used;

    if (free < 8) {
        memzero(&ctx->buffer[used], free);
        (void) md5_body(ctx, ctx->buffer, 64);
        used = 0;
        free = 64;
    }

    memzero(&ctx->buffer[used], free - 8);

    ctx->bytes <<= 3;
    ctx->buffer[56] = (u_char) ctx->bytes;
    ctx->buffer[57] = (u_char) (ctx->bytes >> 8);
    ctx->buffer[58] = (u_char) (ctx->bytes >> 16);
    ctx->buffer[59] = (u_char) (ctx->bytes >> 24);
    ctx->buffer[60] = (u_char) (ctx->bytes >> 32);
    ctx->buffer[61] = (u_char) (ctx->bytes >> 40);
    ctx->buffer[62] = (u_char) (ctx->bytes >> 48);
    ctx->buffer[63] = (u_char) (ctx->bytes >> 56);

    (void) md5_body(ctx, ctx->buffer, 64);

    result[0] = (u_char) ctx->a;
    result[1] = (u_char) (ctx->a >> 8);
    result[2] = (u_char) (ctx->a >> 16);
    result[3] = (u_char) (ctx->a >> 24);
    result[4] = (u_char) ctx->b;
    result[5] = (u_char) (ctx->b >> 8);
    result[6] = (u_char) (ctx->b >> 16);
    result[7] = (u_char) (ctx->b >> 24);
    result[8] = (u_char) ctx->c;
    result[9] = (u_char) (ctx->c >> 8);
    result[10] = (u_char) (ctx->c >> 16);
    result[11] = (u_char) (ctx->c >> 24);
    result[12] = (u_char) ctx->d;
    result[13] = (u_char) (ctx->d >> 8);
    result[14] = (u_char) (ctx->d >> 16);
    result[15] = (u_char) (ctx->d >> 24);

    memzero(ctx, sizeof(*ctx));
}


/*
 * The basic MD5 functions.
 *
 * F and G are optimized compared to their RFC 1321 definitions for
 * architectures that lack an AND-NOT instruction, just like in
 * Colin Plumb's implementation.
 */

#define F(x, y, z)  ((z) ^ ((x) & ((y) ^ (z))))
#define G(x, y, z)  ((y) ^ ((z) & ((x) ^ (y))))
#define H(x, y, z)  ((x) ^ (y) ^ (z))
#define I(x, y, z)  ((y) ^ ((x) | ~(z)))


/*
 * The MD5 transformation for all four rounds.
 */

#define STEP(f, a, b, c, d, x, t, s)                                          \
    (a) += f((b), (c), (d)) + (x) + (t);                                      \
    (a) = (((a) << (s)) | (((a) & 0xffffffff) >> (32 - (s))));                \
    (a) += (b)


/*
 * SET() reads 4 input bytes in little-endian byte order and stores them
 * in a properly aligned word in host byte order.
 *
 * The check for little-endian architectures that tolerate unaligned
 * memory accesses is just an optimization.  Nothing will break if it
 * does not work.
 */

#define SET(n)                                                                \
    (block[n] =                                                               \
    (uint32_t) p[n * 4] |                                                     \
    ((uint32_t) p[n * 4 + 1] << 8) |                                          \
    ((uint32_t) p[n * 4 + 2] << 16) |                                         \
    ((uint32_t) p[n * 4 + 3] << 24))

#define GET(n)      block[n]


/*
 * This processes one or more 64-byte data blocks, but does not update
 * the bit counters.  There are no alignment requirements.
 */

static const u_char *
md5_body(md5_t *ctx, const u_char *data, size_t size)
{
    uint32_t       a, b, c, d;
    uint32_t       saved_a, saved_b, saved_c, saved_d;
    const u_char  *p;
    uint32_t       block[16];

    p = data;

    a = ctx->a;
    b = ctx->b;
    c = ctx->c;
    d = ctx->d;

    do {
        saved_a = a;
        saved_b = b;
        saved_c = c;
        saved_d = d;

        /* Round 1 */

        STEP(F, a, b, c, d, SET(0),  0xd76aa478, 7);
        STEP(F, d, a, b, c, SET(1),  0xe8c7b756, 12);
        STEP(F, c, d, a, b, SET(2),  0x242070db, 17);
        STEP(F, b, c, d, a, SET(3),  0xc1bdceee, 22);
        STEP(F, a, b, c, d, SET(4),  0xf57c0faf, 7);
        STEP(F, d, a, b, c, SET(5),  0x4787c62a, 12);
        STEP(F, c, d, a, b, SET(6),  0xa8304613, 17);
        STEP(F, b, c, d, a, SET(7),  0xfd469501, 22);
        STEP(F, a, b, c, d, SET(8),  0x698098d8, 7);
        STEP(F, d, a, b, c, SET(9),  0x8b44f7af, 12);
        STEP(F, c, d, a, b, SET(10), 0xffff5bb1, 17);
        STEP(F, b, c, d, a, SET(11), 0x895cd7be, 22);
        STEP(F, a, b, c, d, SET(12), 0x6b901122, 7);
        STEP(F, d, a, b, c, SET(13), 0xfd987193, 12);
        STEP(F, c, d, a, b, SET(14), 0xa679438e, 17);
        STEP(F, b, c, d, a, SET(15), 0x49b40821, 22);

        /* Round 2 */

        STEP(G, a, b, c, d, GET(1),  0xf61e2562, 5);
        STEP(G, d, a, b, c, GET(6),  0xc040b340, 9);
        STEP(G, c, d, a, b, GET(11), 0x265e5a51, 14);
        STEP(G, b, c, d, a, GET(0),  0xe9b6c7aa, 20);
        STEP(G, a, b, c, d, GET(5),  0xd62f105d, 5);
        STEP(G, d, a, b, c, GET(10), 0x02441453, 9);
        STEP(G, c, d, a, b, GET(15), 0xd8a1e681, 14);
        STEP(G, b, c, d, a, GET(4),  0xe7d3fbc8, 20);
        STEP(G, a, b, c, d, GET(9),  0x21e1cde6, 5);
        STEP(G, d, a, b, c, GET(14), 0xc33707d6, 9);
        STEP(G, c, d, a, b, GET(3),  0xf4d50d87, 14);
        STEP(G, b, c, d, a, GET(8),  0x455a14ed, 20);
        STEP(G, a, b, c, d, GET(13), 0xa9e3e905, 5);
        STEP(G, d, a, b, c, GET(2),  0xfcefa3f8, 9);
        STEP(G, c, d, a, b, GET(7),  0x676f02d9, 14);
        STEP(G, b, c, d, a, GET(12), 0x8d2a4c8a, 20);

        /* Round 3 */

        STEP(H, a, b, c, d, GET(5),  0xfffa3942, 4);
        STEP(H, d, a, b, c, GET(8),  0x8771f681, 11);
        STEP(H, c, d, a, b, GET(11), 0x6d9d6122, 16);
        STEP(H, b, c, d, a, GET(14), 0xfde5380c, 23);
        STEP(H, a, b, c, d, GET(1),  0xa4beea44, 4);
        STEP(H, d, a, b, c, GET(4),  0x4bdecfa9, 11);
        STEP(H, c, d, a, b, GET(7),  0xf6bb4b60, 16);
        STEP(H, b, c, d, a, GET(10), 0xbebfbc70, 23);
        STEP(H, a, b, c, d, GET(13), 0x289b7ec6, 4);
        STEP(H, d, a, b, c, GET(0),  0xeaa127fa, 11);
        STEP(H, c, d, a, b, GET(3),  0xd4ef3085, 16);
        STEP(H, b, c, d, a, GET(6),  0x04881d05, 23);
        STEP(H, a, b, c, d, GET(9),  0xd9d4d039, 4);
        STEP(H, d, a, b, c, GET(12), 0xe6db99e5, 11);
        STEP(H, c, d, a, b, GET(15), 0x1fa27cf8, 16);
        STEP(H, b, c, d, a, GET(2),  0xc4ac5665, 23);

        /* Round 4 */

        STEP(I, a, b, c, d, GET(0),  0xf4292244, 6);
        STEP(I, d, a, b, c, GET(7),  0x432aff97, 10);
        STEP(I, c, d, a, b, GET(14), 0xab9423a7, 15);
        STEP(I, b, c, d, a, GET(5),  0xfc93a039, 21);
        STEP(I, a, b, c, d, GET(12), 0x655b59c3, 6);
        STEP(I, d, a, b, c, GET(3),  0x8f0ccc92, 10);
        STEP(I, c, d, a, b, GET(10), 0xffeff47d, 15);
        STEP(I, b, c, d, a, GET(1),  0x85845dd1, 21);
        STEP(I, a, b, c, d, GET(8),  0x6fa87e4f, 6);
        STEP(I, d, a, b, c, GET(15), 0xfe2ce6e0, 10);
        STEP(I, c, d, a, b, GET(6),  0xa3014314, 15);
        STEP(I, b, c, d, a, GET(13), 0x4e0811a1, 21);
        STEP(I, a, b, c, d, GET(4),  0xf7537e82, 6);
        STEP(I, d, a, b, c, GET(11), 0xbd3af235, 10);
        STEP(I, c, d, a, b, GET(2),  0x2ad7d2bb, 15);
        STEP(I, b, c, d, a, GET(9),  0xeb86d391, 21);

        a += saved_a;
        b += saved_b;
        c += saved_c;
        d += saved_d;

        p += 64;

    } while (size -= 64);

    ctx->a = a;
    ctx->b = b;
    ctx->c = c;
    ctx->d = d;

    return p;
}


static uint32_t  crc32_table16[] = {
    0x00000000, 0x1db71064, 0x3b6e20c8, 0x26d930ac,
    0x76dc4190, 0x6b6b51f4, 0x4db26158, 0x5005713c,
    0xedb88320, 0xf00f9344, 0xd6d6a3e8, 0xcb61b38c,
    0x9b64c2b0, 0x86d3d2d4, 0xa00ae278, 0xbdbdf21c
};

uint32_t *crc32_table_short = crc32_table16;


static inline uint32_t
crc32_short(u_char *p, size_t len)
{
    u_char    c;
    uint32_t  crc;

    crc = 0xffffffff;

    while (len--) {
        c = *p++;
        crc = crc32_table_short[(crc ^ (c & 0xf)) & 0xf] ^ (crc >> 4);
        crc = crc32_table_short[(crc ^ (c >> 4)) & 0xf] ^ (crc >> 4);
    }

    return crc ^ 0xffffffff;
}


static int
node_cmp( const hashnode_t *a, const hashnode_t *b )
{
    if ( a->point < b->point ) {
        return -1;
    }
    else if ( a->point > b->point ) {
        return 1;
    }
    else {
        return 0;
    }
}


static size_t
point_search( continuum_t *conti, uint32_t p )
{
    /* find the point larger than or equal to p */

    size_t s = 0;
    size_t e = conti->n_nodes - 1;
    size_t mid;

    if ( p <= conti->nodes[ 0 ].point
            || p > conti->nodes[ conti->n_nodes - 1 ].point ) {
        return 0;
    }


    while ( s + 1 < e ) {
        mid = ( s+e ) / 2;
        if ( p <= conti->nodes[ mid ].point ) {
            e = mid;
        }
        else {
            s = mid;
        }
    }

    return e;

}


static void*
get_continuum( lua_State *L, int i )
{
    void **p;

    p = luaL_checkudata( L, i, LUA_CONSIS_HASH_TYPENAME );
    if ( NULL == p ) {
        luaL_argerror( L, i, "Expect " LUA_CONSIS_HASH_TYPENAME );
        return NULL;
    }

    dinfo( "got lua userdata: %p which is pointer: %p", p, *p );

    return p;
}


static int
lch_get( lua_State *L )
{
    void        **p;
    continuum_t  *conti;

    size_t        len;
    const char   *s;
    uint32_t      hash_value;
    size_t        i_node;
    int           n_replica;
    int           i;
    str_t        *name;
    int           i_name;
    int           i_names[ MAX_REPLICA ];
    int           n_names;
    int           exists;


    p = get_continuum( L, 1 );
    conti = *p;

    s = luaL_checklstring( L, 2, &len );

    if ( lua_isnumber( L, 3 ) ) {
        n_replica = lua_tointeger( L, 3 );
        if ( n_replica > MAX_REPLICA ) {
            n_replica = MAX_REPLICA;
        }
        if ( n_replica >= conti->n ) {
            n_replica = conti->n;
        }
    }
    else {
        n_replica = 1;
    }

    derr( "n_replica=%d", n_replica );

    hash_value = ( uint32_t )crc32_short( ( u_char* )s, len );
    hash_value = hash_value >> 18;
    derr( "hash value of %.*s is %d", len, s, hash_value );

    i_node = conti->buckets[ hash_value ];

    for ( i = 0; i < MAX_REPLICA; i++ ) {
        i_names[i] = -1;
    }
    n_names = 0;

    while ( n_names < n_replica ) {

        exists = 0;
        i_name = conti->nodes[ i_node ].name_idx;

        for ( i = 0; i < n_names; i++ ) {
            if ( i_names[i] == i_name ) {
                exists = 1;
                break;
            }
        }

        if ( exists ) {
            i_node++;
            if ( i_node >= conti->n_nodes ) {
                i_node = 0;
            }
        }
        else {
            i_names[ n_names ] = i_name;
            n_names++;

            name = &conti->names[ conti->nodes[ i_node ].name_idx ];
            lua_pushlstring( L, (char*)name->data, name->len );
        }
    }

    return n_replica;
}


static int 
lch_new( lua_State *L ) {

    void        **p;
    continuum_t  *conti;
    const char   *s;
    int           i;
    int           k;
    int           weight;
    int           n;
    int           l;
    str_t        *nm;
    str_t        *name;
    const char   *err;
    size_t        len;
    size_t        size;
    uint32_t      step;
    size_t        i_nodes;
    size_t        n_nodes;
    size_t        n_buckets;
    char         *buf;
    size_t        namebuf_size;
    size_t        namebuf_p;
    char          hashstr_buf[ MAX_NODE_HASH_SRC_LEN ];
    md5_t         ctx;

    /* store md5 result and hash_value at the same place. */
    uint32_t      hash_value[16/sizeof(uint32_t)];
    u_char       *md5_rst = (u_char*)hash_value;


    luaL_checktype( L, 2, LUA_TTABLE );

    n_buckets = N_BUCKETS;

    n = 0;
    n_nodes = 0;
    namebuf_size = 0;
    lua_pushnil( L ); /* iterate all table entries from nil key */

    while ( lua_next( L, -2 ) ) {

        s = lua_tolstring( L, -1, &len );
        if ( NULL == s ) {
            err = lua_pushfstring(L, "Non-string table element at %d", n);
            luaL_argerror( L, 2, err );
            return 0;
        }

        if ( len > MAX_NAME_LEN ) {
            err = lua_pushfstring(L, "String too long: %.*s", len, s);
            luaL_argerror( L, 2, err );
            return 0;
        }

        lua_pop( L, 1 ); /* pop up value, leave key in stack */
        n++;
        namebuf_size += len;
        n_nodes += N_NODE_PER_NAME * 1; /* TODO weights */

        dd( "entry: %s len=%d, total namebuf_size=%d", s, len, namebuf_size );
    }

    if ( 0 == n ) {
        luaL_argerror( L, 2, "Empty table" );
        return 0;
    }


    size = sizeof( continuum_t )
            + sizeof( str_t ) * n
            + namebuf_size
            + sizeof( hashnode_t ) * n_nodes
            + sizeof( size_t ) * n_buckets
            ;

    dd( "total mem size=%d", size );

    buf = malloc( size );
    if ( NULL == buf ) {
        derr( "Failure allocation memory of size=%d", size );
        return 0;
    }

    dinfo( "Allocated continuum mem: %p", buf );

    conti = ( continuum_t* )buf;
    buf += sizeof( continuum_t );

    conti->n = n;

    conti->names = ( str_t* )buf;
    buf += sizeof( str_t ) * n;

    conti->namebuf = buf;
    buf += namebuf_size;

    conti->nodes = ( hashnode_t* )buf;
    buf += sizeof( hashnode_t ) * n_nodes;

    conti->n_nodes = n_nodes;

    conti->buckets = ( size_t* )buf;

    conti->n_buckets = n_buckets;


    i = 0;
    namebuf_p = 0;
    lua_pushnil( L );

    while ( lua_next( L, -2 ) ) {

        s = lua_tolstring( L, -1, &len );

        conti->names[ i ].len = len;
        conti->names[ i ].data = (u_char*)&conti->namebuf[ namebuf_p ];
        conti->names[ i ].used = 0;
        memcpy( conti->names[ i ].data, ( u_char* )s, len );

        dd( "built name: for %d-th name: len=%d name=%.*s", i, len, len, s );

        lua_pop( L, 1 ); /* pop up value, leave key in stack */
        i++;
        namebuf_p += len;
    }


    i_nodes = 0;


    for ( i = 0; i < n; i++ ) {

        weight = N_NODE_PER_NAME;
        name = &conti->names[ i ];

        for ( k = 0; k < weight; k++ ) {
            l = snprintf( hashstr_buf, MAX_NODE_HASH_SRC_LEN,  "%.*s-%ui", (int)name->len, name->data, k );

            md5_init( &ctx );
            md5_update( &ctx, hashstr_buf, l );
            md5_final( md5_rst, &ctx );
            dd( "md5 of %.*s is %x %x", l, hashstr_buf, md5_rst[0], md5_rst[1] );

            /* hash_value = crc32_short( hashstr_buf, l ); */

            conti->nodes[ i_nodes ].point = hash_value[0];
            conti->nodes[ i_nodes ].name_idx = i;

            dd( "built node: hash %p -> %d", hash_value, i );

            i_nodes++;
        }
    }

    qsort( conti->nodes, conti->n_nodes, sizeof( hashnode_t ), ( const void* )node_cmp );


    step = (uint32_t) (0xffffffff / conti->n_buckets);
    for ( i = 0; i < conti->n_buckets; i++ ) {
        i_nodes = point_search( conti, step * i );
        conti->buckets[ i ] = i_nodes;
        k = conti->nodes[ i_nodes ].name_idx;
        nm = &conti->names[ k ];
        nm->used++;
        dd( "bucket %d point to node[%d], to name index=%d, %.*s", i, i_nodes, k, nm->len, nm->data );
    }

    for ( i = 0; i < n; i++ ) {
        nm = &conti->names[ i ];
        dd( "name %.*s used %d times", nm->len, nm->data, nm->used );
    }


    p = lua_newuserdata( L, sizeof( void* ) );
    if ( NULL == p ) {
        dd( "Failure allocate userdata" );
        return 0;
    }

    dinfo( "lua userdata allocated: %p", p );

    *p = conti;

    luaL_getmetatable( L, LUA_CONSIS_HASH_TYPENAME );
    lua_setmetatable( L, -2 );

    return 1;
}


static int 
lch_close( lua_State *L ) {
    void **p;
    dinfo( "gc called" );

    p = get_continuum( L, 1 );
    if ( NULL != *p ) {
        free( *p );
        dinfo( "Freed: %p", *p );
        *p = NULL;
    }
    else {
        dinfo( "continuum pointer is NULL" );
    }

    return 0;
}

static const luaL_Reg lch_funcs[] = {
    { "new",    lch_new },
    { "get",    lch_get },
    { NULL, NULL }
};


int 
luaopen_consistenthash(lua_State *L) {

    luaL_newlib(L, lch_funcs);

    l_table_set_const(L, ERROR_NO_MEMORY);
    l_table_set_const(L, ERROR_INVALID);
    l_table_set_const(L, ERROR_INCOMPLETE);
    l_table_set_const(L, ERROR_FINALIZED);
    l_table_set_const(L, ERROR_UNKNOWN);

    lua_pushliteral(L, "VERSION");
    lua_pushstring(L, LIB_VERSION);
    lua_settable(L, -3);

    luaL_newmetatable(L, LUA_CONSIS_HASH_TYPENAME);

    lua_pushliteral(L, "__index");
    lua_pushvalue(L, -3);
    lua_settable(L, -3);

    lua_pushliteral(L, "__gc");
    lua_pushcfunction(L, lch_close);
    lua_settable(L, -3);

    lua_pop(L, 1);

    return 1;
}


lua_State *init_lua_env()
{
    lua_State  *L;
    char       *fn = (char*)"test-consistenthash.lua";
    const char *errmsg;
    int         rc;

    L = lua_open();
    luaL_openlibs( L );

    rc = luaL_dofile( L, fn );
    if ( 0 != rc )
    {
        errmsg = lua_tostring( L, -1 );
        derr( "Error loading %s: %s\n", fn, errmsg );
        return NULL;
    }

    return L;
}


int
main( int argc, char **argv )
{
    int         rc;
    const char *err;
    const char *s;

    lua_State *L = init_lua_env();
    if ( NULL == L )
    {
        exit( 1 );
    }

    lua_getglobal( L, "main" );

    rc = lua_pcall( L, 0, 3, 0 );
    if ( 0 != rc )
    {
        err = lua_tostring( L, -1 );
        derr( "Error: %s\n", err );
        lua_close(L);
        exit( 1 );
    }

    s = lua_tostring( L, -1 );
    derr( "%s", s );


    int i;
    char buf[ 1024 ];
    int l;

    for ( i = 0; i < 1024*1024; i++ ) {

        lua_pushvalue( L, -3 ); /* func */
        lua_pushvalue( L, -3 ); /* conti */

        l = snprintf( buf, 1024, "%d", i );
        lua_pushlstring( L, buf, l );

        /* lch_get( L ); */

        rc = lua_pcall( L, 2, 1, 0 );
        /*
         * s = lua_tostring( L, -1 );
         * derr( "%s", s );
         */

        lua_pop( L, 1 );
    }

    return 0;
}
