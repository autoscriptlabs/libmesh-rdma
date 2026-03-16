CC = gcc
CFLAGS = -Wall -Wextra -O2 -fPIC \
    -I../../include \
    -I/usr/lib/aarch64-linux-gnu/openmpi/include \
    -I/usr/lib/aarch64-linux-gnu/openmpi/include/openmpi

LDFLAGS = -shared -L../../ -lmesh_rdma -libverbs -lpthread

TARGET = mca_btl_mesh.so

all: $(TARGET)

$(TARGET): btl_mesh.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)
