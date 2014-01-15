#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>

#include "consistent_hash.h"
#include "crc32.h"

#define unused(x) ((void )(x))
#define KEY_BUFFER_LEN	(CH_MAX_KEYLEN + 12)
#define CHASH_MAGIC		0xfee1900d

#ifdef DEBUG
#define dbg_log(fmt, args...) printf(fmt, ##args)
#else
#define dbg_log(fmt, args...)
#endif

/* consistent hash parameters */
static const uint8_t	max_node_num = (uint8_t)((1 << (sizeof(uint8_t)*8)) - 1);
static const uint16_t	max_replicas = (uint16_t)((1 << (sizeof(uint16_t)*8)) - 1);
static const uint8_t 	default_bucket_shift = 7;
static const char		default_delimiter = ':';

static const uint32_t default_replicas = 503;
static const uint32_t default_hash_model = 50119;

static void * default_malloc(size_t size, void *arg)
{
	unused(arg);
	return malloc(size);
}

static void default_free(void *ptr, void *arg)
{
	unused(arg);
	free(ptr);
}

static uint32_t default_hash(const char *s, size_t len)
{
	uint32_t crc = crc32_short(s, len);
	return ((crc & 0x7fffffff) % default_hash_model);
}

void set_malloc(struct chash *ch, void *(*m)(size_t, void*), void*a)
{
	assert(m);
	ch->malloc = m;
	ch->malloc_arg = a;
}

void set_free(struct chash *ch, void (*f)(void *, void*), void*a)
{
	assert(f);
	ch->free = f;
	ch->free_arg = a;
}

void set_node_hash
(struct chash *ch, uint32_t (*h)(const char *, size_t), uint32_t max)
{
	assert(h);
	ch->node_hash = h;
	ch->hash_model = max;
}

void set_key_hash
(struct chash *ch, uint32_t (*h)(const char *, size_t))
{
	assert(h);
	ch->key_hash = h;
}

static inline void * ch_malloc(struct chash *ch, size_t size)
{
	return ch->malloc(size, ch->malloc_arg);
}

static inline void ch_free(struct chash *ch, void *ptr)
{
	return ch->free(ptr, ch->free_arg);
}

static inline int kv_comp(const kv_t *a, const kv_t *b)
{
	//return (a->k > b->k ? 1 : (a->k == b->k ? 0 : -1));
	if (a->k > b->k) return 1;
	if (a->k < b->k) return -1;
	/*
	if (a->v > b->v) return 1;
	if (a->k < b->v) return -1;
	*/
	if (a->s > b->s) return -1;
	if (a->s < b->s) return 1;
	return 0;
}

static int uniq(kv_t *table, size_t n)
{
	int sto, prob;
	uint32_t key;

	key = table[0].k;

	for (sto=1, prob=1; prob<n; prob++) {
		if (table[prob].k != key) {
			key = table[prob].k;
			if (sto != prob) {
				table[sto].k = table[prob].k;
				table[sto].v = table[prob].v;
			}
			sto++;
		}
	}
	return sto;
}

static size_t bin_search(uint32_t p, const kv_t *table, size_t n)
{
    size_t s = 0;
    size_t e = n - 1;
    size_t mid;

    if (p <= table[0].k || p > table[e].k) {
        return 0;
    }

    while (s + 1 < e) {
        mid = (s + e) / 2;
        if (p <= table[ mid ].k) {
            e = mid;
        } else {
            s = mid;
        }
    }

    return e;
}

static inline int linearly_search
(uint32_t p, const kv_t *table, size_t start, size_t stop)
{
	int i;

	for (i=start; i<stop; i++) {
		if (p <= table[i].k) return i;
	}
	return -1;
}

void dump_ring(const struct chash *ch)
{
	int i;

	printf("=================== dump ring ======================\n");
	for (i=0; i<ch->vnodes; i++) {
		printf("%d %u %u\n", i, ch->hash_ring[i].k, ch->hash_ring[i].v);
	}
}

void dump_bucket(const struct chash *ch)
{
	int i;
	int bucket;
	uint32_t pos;

	bucket = ch->hash_model >> ch->bucket_shift;

	printf("=================== dump bucket ======================\n");
	for (i=0; i<bucket; i++) {
		pos = ch->ring_bucket[i];
		printf("%d %u %u %u\n", i, pos,
				ch->hash_ring[pos].k, ch->hash_ring[pos].v);
	}
}

void init_chash(struct chash *ch, uint16_t reps, uint8_t shift, char deli)
{
	assert(ch);

	set_malloc(ch, default_malloc, NULL);
	set_free(ch, default_free, NULL);
	set_node_hash(ch, default_hash, default_hash_model);
	set_key_hash(ch, default_hash);

	ch->replicas = reps == 0 ? default_replicas : reps;
	ch->bucket_shift = shift == 0 ? default_bucket_shift : shift;
	ch->delimiter = deli == 0 ? default_delimiter : deli;

	ch->magic = CHASH_MAGIC;

	return;
}

struct chash * new_chash(uint16_t reps, uint8_t shift, char deli)
{
	struct chash *ch = NULL;

	ch = (struct chash *)calloc(1, sizeof(struct chash));
	if (ch == NULL) return NULL;

	init_chash(ch, reps, shift, deli);

	return ch;
}

int chash_set_nodes(struct chash *ch, str_t *list, uint8_t n)
{
	/* check args */
	if (n > max_node_num || n < 2) return CH_ERR_NODE_NUM;
	if (ch->magic != CHASH_MAGIC) return CH_ERR_NOT_INITED;

	ch->node_list = list;
	ch->ptr = -1;

	/* fill the ring */
	uint32_t vnodes = ch->replicas * n;
	ch->hash_ring = ch_malloc(ch, sizeof(kv_t) * vnodes);
	if (ch->hash_ring == NULL) return CH_ERR_MALLOC;

	int i, j, len, sn=0;
	kv_t *ptr = ch->hash_ring;
	char key_buf[KEY_BUFFER_LEN];
	for (i=0; i<n; i++) {
		for (j=0; j<ch->replicas; j++) {
			len = snprintf(key_buf, KEY_BUFFER_LEN, "%.*s%c%u",
					list[i].len, list[i].ptr, ch->delimiter, j);
			ptr->k = ch->node_hash(key_buf, len);
			ptr->v = i;
			ptr->s = sn;
			sn++;
			ptr++;
		}
	}
	ch->vnodes = vnodes;

	/* sort & uniq the ring */
	qsort(ch->hash_ring, vnodes, sizeof(kv_t), (void*)kv_comp);
	vnodes = ch->vnodes = uniq(ch->hash_ring, vnodes);

	/* fill the bucket */
	uint32_t buckets = ch->hash_model >> ch->bucket_shift;
	ch->ring_bucket = ch_malloc(ch, sizeof(uint32_t) * buckets);
	if (ch->ring_bucket == NULL) {
		ch_free(ch, ch->hash_ring);
		return CH_ERR_MALLOC;
	}

	int pos;
	uint32_t step = 1 << ch->bucket_shift;
	for (i=0; i<buckets; i++) {
		pos = bin_search(i*step, ch->hash_ring, ch->vnodes);
		assert(pos >= 0 && pos < ch->vnodes);
		ch->ring_bucket[i] = pos;
	}

	return CH_OK;
}

void destroy_chash(struct chash *ch)
{
	assert(ch);
	assert(ch->free);
	assert(ch->magic == CHASH_MAGIC);

	if (ch->hash_ring) ch->free(ch->hash_ring, ch->free_arg);
	if (ch->hash_ring) ch->free(ch->ring_bucket, ch->free_arg);
	return;
}

uint8_t get_node(struct chash *ch, const char *str, size_t len)
{
	int pos;
	uint32_t hash;
	uint32_t bucket;

	hash = ch->key_hash(str, len);
	assert(hash < ch->hash_model);
	bucket = hash >> ch->bucket_shift;


	pos = linearly_search(hash, ch->hash_ring,
			ch->ring_bucket[bucket], ch->vnodes);
	dbg_log("key_hash: %u\n", hash);
	dbg_log("bucket: %u\n", bucket);
	dbg_log("pos: %u\n", pos);
	if (pos != -1) {
		ch->ptr = pos;
		return (ch->hash_ring[pos].v);
	}

	ch->ptr = ch->vnodes - 1;
	return next_node(ch);
}

static inline int next_pos(struct chash *ch)
{
	int pos;
	assert(ch->ptr >=0 && ch->ptr < ch->vnodes);
	pos = ch->ptr + 1;

	if (pos < ch->vnodes) {
		ch->ptr = pos;
		dbg_log("nex_pos: %d\n", pos);
		return pos;
	}

	ch->ptr = 0;
	return 0;
}

uint8_t next_node(struct chash *ch)
{
	int next, pos = ch->ptr;

	do {
		next = next_pos(ch);
		dbg_log("nexpos: %d\n", next);
		ch->ptr = next;
	} while (ch->hash_ring[pos].v == ch->hash_ring[next].v);

	return ch->hash_ring[ch->ptr].v;
}

/* WHY NOT CPP? FUCK! */
