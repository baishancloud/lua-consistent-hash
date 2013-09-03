CC= gcc
CFLAGS= -fPIC -Wall -g -O0 -I/usr/include/lua5.1/ -I../
LFLAGS= -shared
LUAOBJS= chash.o
CHOBJS= ../objs/consistent_hash.o ../objs/crc32.o
LUAMOD= "chash_core.so"

.c.o:
	$(CC) $(CFLAGS) -o $@ -c $*.c

all: $(LUAOBJS)
	$(CC) -o ${LUAMOD} $(LFLAGS) $(LIBS) $(LUAOBJS) $(CHOBJS)

$(LUAMOD): $(LUAOBJS)
	$(CC) -o $(LUAMOD) $(LFLAGS) $(LIBS) $(LUAOBJS) $(CHOBJS)

test: $(LUAMOD)
	@lua test.lua

clean:
	@rm *.o *.so
