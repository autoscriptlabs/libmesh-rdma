#ifndef MESH_MPI_INTERNAL_H
#define MESH_MPI_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include "mesh_rdma.h"

#define MESH_MPI_MAX_PROCS   64
#define MESH_MPI_MAX_COMMS   32
#define MESH_MPI_MAX_REQS    1024
#define MESH_MPI_MAX_TYPES   64
#define MESH_MPI_BUF_SIZE    (4 * 1024 * 1024)
#define MESH_MPI_HDR_SIZE    16
#define MESH_MPI_TAG_ANY     (-1)
#define MESH_MPI_SRC_ANY     (-1)

/* Message header */
typedef struct {
    uint16_t src_rank;
    uint16_t tag;
    uint16_t comm_id;
    uint16_t flags;
    uint32_t length;
    uint16_t final_dest;
    uint16_t reserved;
} mesh_mpi_hdr_t;

#define MESH_MPI_FLAG_EAGER  0x0001
#define MESH_MPI_FLAG_RELAY  0x0002

/* Connection to a peer */
typedef struct {
    int direct;
    int next_hop;
    mesh_rdma_conn_t *conn;
    mesh_rdma_mr_t *send_mr;
    mesh_rdma_mr_t *recv_mr;
    char *send_pool;
    char *recv_pool;
    int send_head;
    int nic_idx;
} mesh_mpi_peer_t;

/* Non-blocking request */
typedef struct {
    int active;
    int complete;
    int type;
    int peer_rank;
    int tag;
    int comm_id;
    void *user_buf;
    size_t user_len;
    size_t received_len;
} mesh_mpi_request_t;

/* Communicator — tagged struct so mpi.h can forward-declare it */
struct mesh_mpi_comm_t {
    int id;
    int size;
    int rank;
    int *ranks;
};

/* Datatype */
typedef struct {
    int id;
    size_t size;
    size_t extent;
} mesh_mpi_type_t;

/* Global state */
typedef struct {
    int initialized;
    int rank;
    int size;
    int thread_level;

    mesh_rdma_ctx_t *ctx;
    mesh_mpi_peer_t peers[MESH_MPI_MAX_PROCS];

    int ring_next;
    int ring_prev;
    int ring_route[MESH_MPI_MAX_PROCS];

    mesh_mpi_request_t reqs[MESH_MPI_MAX_REQS];
    pthread_mutex_t req_lock;

    struct {
        mesh_mpi_hdr_t hdr;
        char *data;
        int valid;
    } unexpected[MESH_MPI_MAX_REQS];
    int num_unexpected;

    struct mesh_mpi_comm_t comms[MESH_MPI_MAX_COMMS];
    int num_comms;

    int ctrl_sock;
    char mgmt_iface[64];

    double t_start;
} mesh_mpi_state_t;

extern mesh_mpi_state_t g_mpi;

int mesh_mpi_bootstrap(void);
int mesh_mpi_establish_connections(void);
int mesh_mpi_compute_ring_routes(void);
int mesh_mpi_send_raw(int dest, const void *buf, size_t len,
                      int tag, int comm_id, int flags);
int mesh_mpi_recv_raw(int src, void *buf, size_t len,
                      int *actual_src, int *actual_tag,
                      size_t *actual_len, int tag, int comm_id);
int mesh_mpi_progress(void);
int mesh_mpi_relay_check(void);

#endif
