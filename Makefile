OBJS= crc32.o consistent_hash.o
CC= gcc
CFLAGS = -Wall -fPIC -g -O2 #-DDEBUG
OBJ_DIR= ./objs

.c.o:
	$(CC) $(CFLAGS) -o $(OBJ_DIR)/$@ -c $*.c

all: $(OBJS)
	@echo "built"

lua: $(OBJS)
	cd lua && make

test: $(OBJS) test.o
	cd $(OBJ_DIR) && $(CC) $(CFLAGS) -o test $(OBJS) test.o
	@ $(OBJ_DIR)/test


clean:
	@cd $(OBJ_DIR)
	@rm -f $(OBJS) test.o test
	@cd lua && make clean
