#ifndef __CONSISTENT_HASH_H__
#define __CONSISTENT_HASH_H__
#include <stdint.h>

typedef struct str_s {
	char *			ptr;
	size_t			len;
} str_t;

typedef struct kv_s {
	uint32_t		k;
	uint32_t		v;
	uint32_t		s;
} kv_t;				// 64bit

struct chash {
	str_t *			node_list;
	kv_t *			hash_ring;
	int *			ring_bucket;
	int				ptr;
	uint32_t		vnodes;
	uint32_t		hash_model;

	uint16_t		replicas;
	uint8_t			bucket_shift;
	char			delimiter;

	uint32_t		magic;

	void *			malloc_arg;
	void *			free_arg;
	void *			(*malloc)(size_t, void *arg);
	void			(*free)(void *, void *arg);
	uint32_t		(*node_hash)(const char *, size_t);
	uint32_t		(*key_hash)(const char *, size_t);
};

struct chash * new_chash(uint16_t, uint8_t, char);
void init_chash(struct chash *, uint16_t, uint8_t, char);
void destroy_chash(struct chash *ch);
int chash_set_nodes(struct chash *, str_t *, uint8_t n);

uint8_t get_node(struct chash *ch, const char *str, size_t len);
uint8_t next_node(struct chash *ch);

void set_malloc(struct chash *ch, void *(*)(size_t, void*), void*);
void set_free(struct chash *ch, void (*)(void*, void*), void*);
void set_node_hash(struct chash *ch, uint32_t (*)(const char *, size_t), uint32_t);
void set_key_hash(struct chash *ch, uint32_t (*)(const char *, size_t));
void dump_bucket(const struct chash *ch);
void dump_ring(const struct chash *ch);

#define CH_MAX_KEYLEN		500

#define CH_OK				0
#define	CH_ERR_NODE_NUM		-1
#define CH_ERR_MALLOC		-2
#define CH_ERR_NOT_INITED	-3

#endif
