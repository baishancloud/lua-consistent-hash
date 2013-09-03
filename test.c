#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include "consistent_hash.h"

int main(int argc, char *argv[])
{
	int ret;
	struct chash ch;

	str_t servers[] = {{"10.75.27.83:80", 14}, {"10.75.27.84:80", 14}, {"10.77.120.21:80", 15}, {"10.77.120.23:80", 15}};

	init_chash(&ch, 0, 0, 0);

	ret = chash_set_nodes(&ch, servers, 4);
	assert(ret == 0);

	/*
	dump_ring(&ch);
	dump_bucket(&ch);
	*/

	int i, len;
	char key_buf[256];

	for (i=0; i<1000; i++) {
		len = snprintf(key_buf, 256, "key_%d", i);
		ret = get_node(&ch, key_buf, len);
		printf("%.*s\n", servers[ret].len, servers[ret].ptr);
	}

	destroy_chash(&ch);
	return 0;
}
