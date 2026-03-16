# libmesh-rdma Makefile
# "Because open standards shouldn't require proprietary switches."

CC ?= gcc
CFLAGS = -Wall -Wextra -O2 -fPIC -I./include
LDFLAGS = -libverbs -lpthread

# Library
LIB_SRC = src/mesh_rdma_core.c
LIB_OBJ = $(LIB_SRC:.c=.o)
LIB_SO = libmesh_rdma.so
LIB_A = libmesh_rdma.a

# Tests
TEST_PINGPONG = tests/test_pingpong
TEST_BANDWIDTH = tests/test_bandwidth

.PHONY: all clean install test

all: $(LIB_SO) $(LIB_A) $(TEST_PINGPONG) $(TEST_BANDWIDTH)

# Shared library
$(LIB_SO): $(LIB_OBJ)
	$(CC) -shared -o $@ $^ $(LDFLAGS)

# Static library
$(LIB_A): $(LIB_OBJ)
	ar rcs $@ $^

# Object files
%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# Pingpong test
$(TEST_PINGPONG): tests/test_pingpong.c $(LIB_A)
	$(CC) $(CFLAGS) -o $@ $< -L. -l:$(LIB_A) $(LDFLAGS)

# Bandwidth test
$(TEST_BANDWIDTH): tests/test_bandwidth.c $(LIB_A)
	$(CC) $(CFLAGS) -o $@ $< -L. -l:$(LIB_A) $(LDFLAGS)

clean:
	rm -f $(LIB_OBJ) $(LIB_SO) $(LIB_A) $(TEST_PINGPONG) $(TEST_BANDWIDTH)

install: $(LIB_SO) $(LIB_A)
	install -d $(DESTDIR)/usr/local/lib
	install -d $(DESTDIR)/usr/local/include
	install -m 755 $(LIB_SO) $(DESTDIR)/usr/local/lib/
	install -m 644 $(LIB_A) $(DESTDIR)/usr/local/lib/
	install -m 644 include/mesh_rdma.h $(DESTDIR)/usr/local/include/
	ldconfig

test: $(TEST_PINGPONG)
	@echo ""
	@echo "Run on node A:  ./tests/test_pingpong -s"
	@echo "Run on node B:  ./tests/test_pingpong -c <node-A-ip>"
	@echo ""
