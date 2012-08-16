OMIT_FRAME_PTR = -fomit-frame-pointer

LUAPKG = lua5.2
CFLAGS = `pkg-config $(LUAPKG) --cflags` -fPIC -O3 -Wall $(OMIT_FRAME_PTR)
LFLAGS = -shared


## If your system doesn't have pkg-config or if you do not want to get the
## install path from Lua, comment out the previous lines and uncomment and
## change the following ones according to your building environment.

#CFLAGS = -I/usr/local/include/ -fPIC -O3 -Wall $(OMIT_FRAME_PTR)
#LFLAGS = -shared
#INSTALL_PATH = /usr/local/lib/lua/5.2/


all: lib

consistenthash.lo: consistenthash.c
	$(CC) -o consistenthash.lo -c $(CFLAGS) consistenthash.c

consistenthash.so: consistenthash.lo
	$(CC) -o consistenthash.so $(LFLAGS) $(LIBS) consistenthash.lo

lib: consistenthash.c
	$(CC) -o consistenthash.so consistenthash.c $(LFLAGS) $(LIBS)
	lua testtt.lua

standalone: consistenthash.c
	$(CC) -o consistenthash consistenthash.c -llua $(LIBS)

clean:
	$(RM) consistenthash.so consistenthash.lo

test: consistenthash.so test_tt.lua
	lua test_tt.lua

