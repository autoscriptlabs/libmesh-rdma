/*
 * libmesh-rdma: Universal RDMA for Consumer Hardware
 *
 * Because open standards shouldn't require proprietary switches.
 *
 * MIT License - do what you want with it.
 */

#ifndef MESH_RDMA_H
#define MESH_RDMA_H

#include <stdint.h>
#include <stddef.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Configuration
 * ============================================================ */

#define MESH_RDMA_MAX_NICS      8
#define MESH_RDMA_MAX_SEND_WR   64
#define MESH_RDMA_MAX_RECV_WR   64
#define MESH_RDMA_MAX_SGE       1
#define MESH_RDMA_MAX_INLINE    64
#define MESH_RDMA_CQ_DEPTH      4096
#define MESH_RDMA_HANDLE_SIZE   128

/* ============================================================
 * Log levels
 * ============================================================ */

#define MESH_RDMA_LOG_ERROR   0
#define MESH_RDMA_LOG_WARN    1
#define MESH_RDMA_LOG_INFO    2
#define MESH_RDMA_LOG_DEBUG   3

/* User-provided log callback */
typedef void (*mesh_rdma_log_fn)(int level, const char *fmt, ...);

/* ============================================================
 * NIC information (read-only after init)
 * ============================================================ */

typedef struct mesh_rdma_nic {
    /* RDMA resources */
    struct ibv_context *context;
    struct ibv_pd      *pd;
    int                 port_num;
    int                 gid_index;      /* Auto-discovered IPv4-mapped GID */
    union ibv_gid       gid;            /* Cached GID value */
    enum ibv_mtu        active_mtu;

    /* Network addressing */
    uint32_t            ip_addr;        /* Host byte order */
    uint32_t            netmask;        /* Host byte order */
    uint32_t            subnet;         /* ip_addr & netmask */

    /* Device identification */
    char                rdma_name[64];  /* e.g., "rocep1s0f1" */
    char                net_name[64];   /* e.g., "enp1s0f1np1" */

    /* Capabilities */
    int                 max_qp;
    int                 max_mr;
    int                 link_speed_mbps;
} mesh_rdma_nic_t;

/* ============================================================
 * Connection handle — exchanged between peers via TCP OOB
 * Must fit in MESH_RDMA_HANDLE_SIZE bytes
 * ============================================================ */

typedef struct mesh_rdma_handle {
    uint32_t        magic;              /* 0x4D524D48 = "MRMH" */
    uint16_t        handshake_port;     /* TCP port for QP exchange */
    uint16_t        qp_num;
    uint32_t        psn;
    uint8_t         gid_index;
    uint8_t         nic_idx;
    uint8_t         mtu;
    uint8_t         num_addrs;
    union ibv_gid   gid;                /* 16 bytes */
    /* Per-NIC address entries for subnet-aware routing */
    struct {
        uint32_t    ip;                 /* Network byte order */
        uint32_t    mask;               /* Network byte order */
        uint16_t    qp_num;
        uint8_t     nic_idx;
        uint8_t     gid_index;
    } addrs[6];
} mesh_rdma_handle_t;

/* ============================================================
 * QP info — exchanged during handshake
 * ============================================================ */

typedef struct mesh_rdma_qp_info {
    uint32_t        qp_num;             /* Network byte order */
    uint32_t        psn;                /* Network byte order */
    uint8_t         gid[16];            /* Raw GID */
    uint32_t        ip;                 /* Network byte order */
    uint8_t         gid_index;
    uint8_t         nic_idx;
    uint8_t         mtu;
    uint8_t         reserved;
} mesh_rdma_qp_info_t;

/* ============================================================
 * Connection — an established RDMA connection to a peer
 * ============================================================ */

typedef struct mesh_rdma_conn {
    mesh_rdma_nic_t *nic;
    struct ibv_qp   *qp;
    struct ibv_cq   *cq;
    uint32_t         remote_qp_num;
    union ibv_gid    remote_gid;
    int              connected;
} mesh_rdma_conn_t;

/* ============================================================
 * Memory registration handle
 * ============================================================ */

typedef struct mesh_rdma_mr {
    struct ibv_mr   *mr;
    void            *addr;
    size_t           len;
} mesh_rdma_mr_t;

/* ============================================================
 * Context — global library state
 * ============================================================ */

typedef struct mesh_rdma_ctx {
    mesh_rdma_nic_t nics[MESH_RDMA_MAX_NICS];
    int             num_nics;
    int             initialized;

    /* Configuration */
    int             gid_index_override; /* -1 = auto-detect (default) */
    int             debug_level;
    int             timeout_ms;
    int             retry_count;

    /* Logging */
    mesh_rdma_log_fn log_fn;
} mesh_rdma_ctx_t;

/* ============================================================
 * Core API
 * ============================================================ */

/*
 * Initialize the library. Discovers all RDMA-capable NICs,
 * auto-detects IPv4-mapped GIDs, allocates protection domains.
 *
 * Environment variables:
 *   MESH_RDMA_GID_INDEX     Override GID index (-1 = auto, default)
 *   MESH_RDMA_DEBUG         Debug level (0-3, default 0)
 *   MESH_RDMA_TIMEOUT_MS    Connection timeout (default 5000)
 *   MESH_RDMA_RETRY_COUNT   Connection retries (default 3)
 */
int mesh_rdma_init(mesh_rdma_ctx_t **ctx_out);
void mesh_rdma_destroy(mesh_rdma_ctx_t *ctx);

/* Set log callback (optional, must call before init) */
void mesh_rdma_set_log(mesh_rdma_ctx_t *ctx, mesh_rdma_log_fn fn);

/* ============================================================
 * NIC Query
 * ============================================================ */

int mesh_rdma_num_nics(mesh_rdma_ctx_t *ctx);
mesh_rdma_nic_t *mesh_rdma_get_nic(mesh_rdma_ctx_t *ctx, int idx);
mesh_rdma_nic_t *mesh_rdma_find_nic_for_ip(mesh_rdma_ctx_t *ctx, uint32_t peer_ip);

/* ============================================================
 * Connection Setup
 * ============================================================ */

/*
 * Create a listening endpoint. Fills handle_out with connection
 * info that the remote peer needs to connect.
 */
int mesh_rdma_listen(mesh_rdma_ctx_t *ctx, int nic_idx,
                     mesh_rdma_handle_t *handle_out,
                     void **listen_state);

/*
 * Connect to a remote peer using their handle.
 * Performs TCP handshake to exchange QP info, then transitions
 * QP through INIT → RTR → RTS.
 */
int mesh_rdma_connect(mesh_rdma_ctx_t *ctx,
                      mesh_rdma_handle_t *remote_handle,
                      mesh_rdma_conn_t **conn_out);

/*
 * Accept an incoming connection from a remote peer.
 * Blocks until a peer connects via handshake.
 */
int mesh_rdma_accept(mesh_rdma_ctx_t *ctx,
                     void *listen_state,
                     mesh_rdma_conn_t **conn_out);

/* Close a connection (graceful QP teardown) */
void mesh_rdma_close(mesh_rdma_conn_t *conn);

/* Stop listening */
void mesh_rdma_close_listen(void *listen_state);

/* ============================================================
 * Memory Registration
 * ============================================================ */

int mesh_rdma_reg_mr(mesh_rdma_ctx_t *ctx, int nic_idx,
                     void *buf, size_t len,
                     mesh_rdma_mr_t **mr_out);
void mesh_rdma_dereg_mr(mesh_rdma_mr_t *mr);

/* ============================================================
 * Data Transfer
 * ============================================================ */

/* Post a send. Returns 0 on success. */
int mesh_rdma_post_send(mesh_rdma_conn_t *conn,
                        void *buf, size_t len, uint32_t lkey,
                        uint64_t wr_id);

/* Post a receive buffer. Returns 0 on success. */
int mesh_rdma_post_recv(mesh_rdma_conn_t *conn,
                        void *buf, size_t len, uint32_t lkey,
                        uint64_t wr_id);

/* Poll for completion. Sets *done=1 if complete, *bytes = transfer size. */
int mesh_rdma_poll(mesh_rdma_conn_t *conn, int *done, size_t *bytes);

/* ============================================================
 * GID Utilities (exposed for diagnostics)
 * ============================================================ */

int mesh_rdma_gid_is_ipv4_mapped(const union ibv_gid *gid);
int mesh_rdma_gid_is_link_local(const union ibv_gid *gid);
uint32_t mesh_rdma_gid_to_ipv4(const union ibv_gid *gid);
int mesh_rdma_find_ipv4_gid_index(struct ibv_context *ctx,
                                   int port_num,
                                   uint32_t expected_ip);

/* ============================================================
 * TCP Handshake Utilities (exposed for plugin authors)
 * ============================================================ */

int mesh_rdma_create_handshake_socket(uint32_t bind_ip, uint16_t *port_out);
int mesh_rdma_accept_handshake(int listen_sock,
                               mesh_rdma_qp_info_t *remote_info,
                               mesh_rdma_qp_info_t *local_info);
int mesh_rdma_send_handshake(uint32_t remote_ip, uint16_t remote_port,
                             mesh_rdma_qp_info_t *local_info,
                             mesh_rdma_qp_info_t *remote_info);

/* ============================================================
 * QP State Machine (exposed for plugin authors)
 * ============================================================ */

/* Create QP in INIT state */
int mesh_rdma_create_qp(mesh_rdma_nic_t *nic,
                        struct ibv_qp **qp_out,
                        struct ibv_cq **cq_out);

/* Transition QP: INIT → RTR → RTS with retry */
int mesh_rdma_connect_qp(struct ibv_qp *qp,
                         mesh_rdma_nic_t *nic,
                         mesh_rdma_qp_info_t *remote);

#ifdef __cplusplus
}
#endif

#endif /* MESH_RDMA_H */
