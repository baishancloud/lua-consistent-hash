#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>

#include <lua.h>
#include <lauxlib.h>

#include "consistent_hash.h"

#define CONSISTENT_HASH_MODULE		"chash_core"

#define CH_LUA_API_DEFINE(x, y) static int x(lua_State *y)

CH_LUA_API_DEFINE(lch_new, L)
{
	int			i, ret;
	size_t		n;
	str_t *		nodes;
	struct chash *ch;

	/* check table */
	luaL_checktype(L, 2, LUA_TTABLE);

	n = lua_objlen(L, 2);
	if (n == 0) {
		luaL_error(L, "no nodes found");
		return 0;
	}


	nodes = (str_t *)calloc(n, sizeof(str_t));
	if (nodes == NULL) {
		luaL_error(L, "alloc node list failed");
		return 0;
	}

	lua_pushnil(L);
	for (i=0; i<n; i++) {
		lua_next(L, -2);
		nodes[i].ptr = (char *)lua_tolstring(L, -1, &nodes[i].len);
		if (nodes[i].len > CH_MAX_KEYLEN) {
			luaL_error(L, "%s is tooo long", nodes[i].len);
			free(nodes);
			return 0;
		}

		lua_pop(L, 1);
	}

	ch = (struct chash *)lua_newuserdata(L, sizeof(struct chash));
	if (ch == NULL) {
		free(nodes);
		luaL_error(L, "alloc chash failed");
		return 0;
	}

	init_chash(ch, 0, 0, 0);

	ret = chash_set_nodes(ch, nodes, n);
	free(nodes);

	if (ret != CH_OK) {
		luaL_error(L, "init chash failed");
		return 0;
	}

	luaL_getmetatable(L, CONSISTENT_HASH_MODULE);
	lua_setmetatable( L, -2 );

	return 1;
}

CH_LUA_API_DEFINE(lch_get, L)
{
	uint8_t n;
	size_t len;
	const char *key;
	struct chash *ch;

	ch = (struct chash *)luaL_checkudata(L, 1, CONSISTENT_HASH_MODULE);

	key = luaL_checklstring(L, -1, &len);

	n = get_node(ch, key, len);
	lua_pushinteger(L, n);

	return 1;
}

CH_LUA_API_DEFINE(lch_next, L)
{
	uint8_t n;
	struct chash *ch;

	ch = (struct chash *)luaL_checkudata(L, 1, CONSISTENT_HASH_MODULE);

	n = next_node(ch);
	lua_pushnumber(L, n);

	return 1;
}

CH_LUA_API_DEFINE(lch_dump_ring, L)
{
	struct chash *ch;

	ch = (struct chash *)luaL_checkudata(L, 1, CONSISTENT_HASH_MODULE);

	dump_ring(ch);
	return 0;
}

CH_LUA_API_DEFINE(lch_dump_bucket, L)
{
	struct chash *ch;

	ch = (struct chash *)luaL_checkudata(L, 1, CONSISTENT_HASH_MODULE);

	dump_bucket(ch);
	return 0;
}

CH_LUA_API_DEFINE(lch_free, L)
{
	struct chash *ch;

	ch = (struct chash *)luaL_checkudata(L, 1, CONSISTENT_HASH_MODULE);

	if (ch) {
		destroy_chash(ch);
	}

	return 0;
}

static const luaL_reg chash_f[] = {
	{"new",			lch_new},
	{NULL,			NULL},
};

static const luaL_reg chash_m[] = {
	{"get",			lch_get},
	{"next",		lch_next},
	{"dump_ring",	lch_dump_ring},
	{"dump_buckep",	lch_dump_bucket},
	{"__gc",		lch_free},
	{NULL,			NULL},
};

int luaopen_chash_core(lua_State *L)
{

	luaL_newmetatable(L, CONSISTENT_HASH_MODULE);

	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");

	luaL_register(L, NULL, chash_m);
	luaL_register(L, "chash_core", chash_f);

	return 1;
}
