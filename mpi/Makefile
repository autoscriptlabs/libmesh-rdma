CC = gcc
CFLAGS = -Wall -Wextra -O2 -fPIC -I. -I../include
LDFLAGS = -shared -L.. -lmesh_rdma -libverbs -lpthread

SRCS = mesh_mpi.c mesh_mpi_pt2pt.c mesh_mpi_coll.c
OBJS = $(SRCS:.c=.o)
TARGET = libmesh_mpi.so

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)
