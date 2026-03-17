/*
 * mesh_mpi.c — Bootstrap, Init, Finalize for libmesh-mpi
 *
 * Rank 0 runs a TCP rendezvous server on the management network.
 * All ranks connect, exchange RDMA handles, establish ring connections.
 * Non-adjacent pairs relay through ring neighbors.
 *
 * Environment variables:
 *   MESH_MPI_RANK      - this process's rank
 *   MESH_MPI_SIZE      - world size
 *   MESH_MPI_CTRL_ADDR - rank 0's management IP:port
 *   MESH_MPI_MGMT_IF   - management network interface (default: enP7s7)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <ifaddrs.h>
#include <signal.h>
#include <ifaddrs.h>

#include "mesh_mpi_internal.h"
#include "mpi.h"

/* Alarm handler for accept timeout */
static void alarm_handler(int sig) { (void)sig; }

/* Listen states saved from bootstrap for accept */
static void *listen_states[8];

/* Global state */
mesh_mpi_state_t g_mpi = {0};

/* Predefined communicator storage */
static struct mesh_mpi_comm_t g_comm_world;
static struct mesh_mpi_comm_t g_comm_self;
static int g_comm_world_ranks[MESH_MPI_MAX_PROCS];

/* Predefined objects */
MPI_Comm MPI_COMM_WORLD = &g_comm_world;
MPI_Comm MPI_COMM_SELF  = &g_comm_self;
MPI_Comm MPI_COMM_NULL  = NULL;

/* ============================================================
 * TCP helpers for bootstrap
 * ============================================================ */

static int tcp_send_all(int sock, const void *buf, size_t len) {
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(sock, p, len, 0);
        if (n <= 0) return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int tcp_recv_all(int sock, void *buf, size_t len) {
    char *p = buf;
    while (len > 0) {
        ssize_t n = recv(sock, p, len, MSG_WAITALL);
        if (n <= 0) return -1;
        p += n;
        len -= n;
    }
    return 0;
}

/* ============================================================
 * Bootstrap: rank 0 collects all handles, broadcasts back
 *
 * Protocol:
 *   1. Each rank connects to rank 0 via TCP
 *   2. Sends: {rank, num_nics, nic_handles[]}
 *   3. Rank 0 collects all, broadcasts the complete table
 *   4. Each rank establishes RDMA connections to ring neighbors
 * ============================================================ */

typedef struct {
    int rank;
    int num_nics;
    struct {
        mesh_rdma_handle_t handle;
        uint32_t ip_addr;
        int nic_idx;
    } nics[8];
    uint32_t mgmt_ip;
} bootstrap_msg_t;

static int bootstrap_server(bootstrap_msg_t *all_msgs) {
    int listen_sock, opt = 1;
    struct sockaddr_in addr;

    listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    /* Parse port from MESH_MPI_CTRL_ADDR */
    char *ctrl = getenv("MESH_MPI_CTRL_ADDR");
    int port = 19900;
    if (ctrl) {
        char *colon = strchr(ctrl, ':');
        if (colon) port = atoi(colon + 1);
    }
    addr.sin_port = htons(port);

    if (bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[mesh-mpi] rank 0: bind failed: %s\n", strerror(errno));
        return -1;
    }
    listen(listen_sock, 32);
    fprintf(stderr, "[mesh-mpi] rank 0: listening on port %d for %d peers\n",
            port, g_mpi.size - 1);

    /* Collect from self */
    memcpy(&all_msgs[0], &all_msgs[g_mpi.rank], sizeof(bootstrap_msg_t));

    /* Accept from all other ranks */
    for (int i = 0; i < g_mpi.size - 1; i++) {
        struct sockaddr_in peer_addr;
        socklen_t peer_len = sizeof(peer_addr);
        int csock = accept(listen_sock, (struct sockaddr *)&peer_addr, &peer_len);
        if (csock < 0) {
            fprintf(stderr, "[mesh-mpi] rank 0: accept failed: %s\n", strerror(errno));
            return -1;
        }

        bootstrap_msg_t msg;
        if (tcp_recv_all(csock, &msg, sizeof(msg)) != 0) {
            fprintf(stderr, "[mesh-mpi] rank 0: recv from peer failed\n");
            close(csock);
            return -1;
        }

        fprintf(stderr, "[mesh-mpi] rank 0: received bootstrap from rank %d (%d NICs)\n",
                msg.rank, msg.num_nics);
        memcpy(&all_msgs[msg.rank], &msg, sizeof(msg));

        /* Send back the full table once we have everyone */
        if (i == g_mpi.size - 2) {
            /* Broadcast complete table to all connected peers */
            /* We need to re-accept or keep sockets. Simpler: use a two-phase protocol. */
        }
        close(csock);
    }

    close(listen_sock);
    return 0;
}

int mesh_mpi_bootstrap(void) {
    /* Read environment */
    char *s_rank = getenv("MESH_MPI_RANK");
    char *s_size = getenv("MESH_MPI_SIZE");
    char *s_ctrl = getenv("MESH_MPI_CTRL_ADDR");
    char *s_mgmt = getenv("MESH_MPI_MGMT_IF");

    if (!s_rank || !s_size || !s_ctrl) {
        fprintf(stderr, "[mesh-mpi] Missing MESH_MPI_RANK, MESH_MPI_SIZE, or MESH_MPI_CTRL_ADDR\n");
        return -1;
    }

    g_mpi.rank = atoi(s_rank);
    g_mpi.size = atoi(s_size);
    strncpy(g_mpi.mgmt_iface, s_mgmt ? s_mgmt : "enP7s7", sizeof(g_mpi.mgmt_iface) - 1);

    fprintf(stderr, "[mesh-mpi] rank %d/%d starting bootstrap (ctrl=%s)\n",
            g_mpi.rank, g_mpi.size, s_ctrl);

    /* Initialize RDMA */
    if (mesh_rdma_init(&g_mpi.ctx) != 0) {
        fprintf(stderr, "[mesh-mpi] rank %d: RDMA init failed\n", g_mpi.rank);
        return -1;
    }

    /* Build local bootstrap message — advertise all NICs with IPs */
    bootstrap_msg_t my_msg = {0};
    my_msg.rank = g_mpi.rank;
    my_msg.num_nics = 0;
    /* Get our management IP from ctrl addr connection */
    {
        char *ctrl = getenv("MESH_MPI_CTRL_ADDR");
        if (ctrl && g_mpi.rank == 0) {
            /* Rank 0: parse from env */
            char ip_str[64];
            sscanf(ctrl, "%63[^:]", ip_str);
            struct in_addr a;
            inet_aton(ip_str, &a);
            my_msg.mgmt_ip = ntohl(a.s_addr);
        } else {
            /* Other ranks: get our management IP from the interface */
            struct ifaddrs *ifap, *ifa;
            getifaddrs(&ifap);
            for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
                if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
                uint32_t ip = ntohl(((struct sockaddr_in *)ifa->ifa_addr)->sin_addr.s_addr);
                if ((ip >> 24) == 10) { my_msg.mgmt_ip = ip; break; }
            }
            freeifaddrs(ifap);
        }
    }

    for (int i = 0; i < mesh_rdma_num_nics(g_mpi.ctx); i++) {
        mesh_rdma_nic_t *nic = mesh_rdma_get_nic(g_mpi.ctx, i);
        if (nic->ip_addr == 0) continue;

        mesh_rdma_handle_t handle;
        void *listen_state;
        if (mesh_rdma_listen(g_mpi.ctx, i, &handle, &listen_state) != 0) continue;

        int idx = my_msg.num_nics++;
        my_msg.nics[idx].handle = handle;
        my_msg.nics[idx].ip_addr = nic->ip_addr;
        my_msg.nics[idx].nic_idx = i;

        /* Save listen state for later accept calls */
        listen_states[idx] = listen_state;

        fprintf(stderr, "[mesh-mpi] rank %d: NIC %d (%s) IP=%d.%d.%d.%d\n",
                g_mpi.rank, i, nic->rdma_name,
                (nic->ip_addr >> 24) & 0xFF, (nic->ip_addr >> 16) & 0xFF,
                (nic->ip_addr >> 8) & 0xFF, nic->ip_addr & 0xFF);
    }

    /* Parse ctrl address */
    char ctrl_ip[64];
    int ctrl_port = 19900;
    strncpy(ctrl_ip, s_ctrl, sizeof(ctrl_ip) - 1);
    char *colon = strchr(ctrl_ip, ':');
    if (colon) {
        *colon = 0;
        ctrl_port = atoi(colon + 1);
    }

    /* Two-phase bootstrap: gather then scatter */
    bootstrap_msg_t *all_msgs = calloc(g_mpi.size, sizeof(bootstrap_msg_t));
    if (!all_msgs) return -1;

    if (g_mpi.rank == 0) {
        /* Phase 1: Rank 0 accepts connections, collects all bootstrap messages */
        all_msgs[0] = my_msg;

        int listen_sock, opt = 1;
        struct sockaddr_in addr;

        listen_sock = socket(AF_INET, SOCK_STREAM, 0);
        setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(ctrl_port);

        if (bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            fprintf(stderr, "[mesh-mpi] rank 0: bind port %d failed: %s\n",
                    ctrl_port, strerror(errno));
            free(all_msgs);
            return -1;
        }
        listen(listen_sock, 32);
        fprintf(stderr, "[mesh-mpi] rank 0: listening on port %d\n", ctrl_port);

        int *peer_socks = calloc(g_mpi.size, sizeof(int));
        for (int i = 0; i < g_mpi.size - 1; i++) {
            struct sockaddr_in pa;
            socklen_t pl = sizeof(pa);
            int cs = accept(listen_sock, (struct sockaddr *)&pa, &pl);
            if (cs < 0) { free(all_msgs); free(peer_socks); return -1; }

            bootstrap_msg_t msg;
            tcp_recv_all(cs, &msg, sizeof(msg));
            all_msgs[msg.rank] = msg;
            peer_socks[msg.rank] = cs;

            fprintf(stderr, "[mesh-mpi] rank 0: got bootstrap from rank %d\n", msg.rank);
        }

        /* Phase 2: Send complete table back to everyone */
        size_t table_size = g_mpi.size * sizeof(bootstrap_msg_t);
        for (int i = 1; i < g_mpi.size; i++) {
            tcp_send_all(peer_socks[i], all_msgs, table_size);
            close(peer_socks[i]);
        }
        close(listen_sock);
        free(peer_socks);

    } else {
        /* Non-zero ranks: connect to rank 0, send our msg, receive table */
        int sock = -1;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(ctrl_port);
        inet_pton(AF_INET, ctrl_ip, &addr.sin_addr);

        /* Retry connection — rank 0 might not be listening yet */
        for (int retry = 0; retry < 100; retry++) {
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) break;
            close(sock);
            sock = -1;
            usleep(100000); /* 100ms */
        }
        if (sock < 0) {
            fprintf(stderr, "[mesh-mpi] rank %d: connect to rank 0 failed\n", g_mpi.rank);
            free(all_msgs);
            return -1;
        }

        /* Send our bootstrap message */
        tcp_send_all(sock, &my_msg, sizeof(my_msg));

        /* Receive complete table */
        tcp_recv_all(sock, all_msgs, g_mpi.size * sizeof(bootstrap_msg_t));
        close(sock);
    }

    fprintf(stderr, "[mesh-mpi] rank %d: bootstrap table received (%d ranks)\n",
            g_mpi.rank, g_mpi.size);

    /* Phase 3: Establish RDMA connections to ring neighbors */
    g_mpi.ring_next = (g_mpi.rank + 1) % g_mpi.size;
    g_mpi.ring_prev = (g_mpi.rank + g_mpi.size - 1) % g_mpi.size;

    /* For each ring neighbor, find matching subnet NIC pair */
    /* Connect in two phases to avoid deadlock:
     * Phase 1: even ranks accept from ring_next, odd ranks connect to ring_prev
     * Phase 2: swap roles */
    int phase_peers[2];
    if (g_mpi.rank % 2 == 0) {
        phase_peers[0] = g_mpi.ring_next;  /* accept */
        phase_peers[1] = g_mpi.ring_prev;  /* connect */
    } else {
        phase_peers[0] = g_mpi.ring_prev;  /* connect */
        phase_peers[1] = g_mpi.ring_next;  /* accept */
    }
    int neighbors[2] = { phase_peers[0], phase_peers[1] };
    if (1) {
    for (int n = 0; n < 2; n++) {
        int peer = neighbors[n];

        /* Skip if already connected (happens in 2-node ring) */
        if (g_mpi.peers[peer].direct) {
            fprintf(stderr, "[mesh-mpi] rank %d: already connected to %d, skipping\n",
                    g_mpi.rank, peer);
            continue;
        }

        bootstrap_msg_t *peer_msg = &all_msgs[peer];
        int connected = 0;

        /* Find a NIC pair on the same subnet */
        for (int li = 0; li < my_msg.num_nics && !connected; li++) {
            uint32_t my_subnet = my_msg.nics[li].ip_addr & 0xFFFFFF00;
            for (int ri = 0; ri < peer_msg->num_nics && !connected; ri++) {
                uint32_t peer_subnet = peer_msg->nics[ri].ip_addr & 0xFFFFFF00;
                if (my_subnet != peer_subnet) continue;

                /* Matching subnet! Establish RDMA connection. */
                fprintf(stderr, "[mesh-mpi] rank %d: connecting to rank %d "
                        "(NIC %d.%d.%d.%d <-> %d.%d.%d.%d)\n",
                        g_mpi.rank, peer,
                        (my_msg.nics[li].ip_addr >> 24) & 0xFF,
                        (my_msg.nics[li].ip_addr >> 16) & 0xFF,
                        (my_msg.nics[li].ip_addr >> 8) & 0xFF,
                        my_msg.nics[li].ip_addr & 0xFF,
                        (peer_msg->nics[ri].ip_addr >> 24) & 0xFF,
                        (peer_msg->nics[ri].ip_addr >> 16) & 0xFF,
                        (peer_msg->nics[ri].ip_addr >> 8) & 0xFF,
                        peer_msg->nics[ri].ip_addr & 0xFF);

                /* Lower rank accepts, higher rank connects */
                mesh_mpi_peer_t *p = &g_mpi.peers[peer];
                p->nic_idx = my_msg.nics[li].nic_idx;

                if (g_mpi.rank < peer) {
                    /* Accept with timeout on listen socket */
                    void *ls = listen_states[li];
                    /* ls is opaque — it contains the listen sock fd
                     * Set SO_RCVTIMEO on it before accept */
                    int listen_fd = *(int *)((char *)ls + 2 * sizeof(void *));  /* sock after qp,cq pointers */
                    struct timeval tv = { .tv_sec = 3, .tv_usec = 0 };
                    setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                    int acc_ret = mesh_rdma_accept(g_mpi.ctx, ls, &p->conn);
                    if (acc_ret != 0) {
                        fprintf(stderr, "[mesh-mpi] rank %d: accept from rank %d failed/timeout\n",
                                g_mpi.rank, peer);
                        continue;
                    }
                } else {
                    /* We connect — create QP on our known-correct NIC
                     * (NOT mesh_rdma_connect which re-selects NIC internally
                     * and may pick the wrong one if netmasks differ from /24) */
                    mesh_rdma_nic_t *local_nic = mesh_rdma_get_nic(g_mpi.ctx, p->nic_idx);
                    struct ibv_qp *qp;
                    struct ibv_cq *cq;
                    if (mesh_rdma_create_qp(local_nic, &qp, &cq) != 0) {
                        fprintf(stderr, "[mesh-mpi] rank %d: QP create for rank %d failed\n",
                                g_mpi.rank, peer);
                        continue;
                    }

                    /* Prepare local QP info using our NIC */
                    mesh_rdma_qp_info_t local_info, remote_info;
                    memset(&local_info, 0, sizeof(local_info));
                    local_info.qp_num = htonl(qp->qp_num);
                    local_info.psn = htonl(0);
                    memcpy(local_info.gid, &local_nic->gid, 16);
                    local_info.gid_index = local_nic->gid_index;
                    local_info.mtu = local_nic->active_mtu;

                    /* TCP handshake via management network */
                    uint32_t hs_ip = peer_msg->mgmt_ip;
                    uint16_t hs_port = peer_msg->nics[ri].handle.handshake_port;
                    fprintf(stderr, "[mesh-mpi] rank %d: connect to rank %d — "
                            "local NIC %d (%s), QP %u, handshake via %d.%d.%d.%d:%d\n",
                            g_mpi.rank, peer, p->nic_idx, local_nic->rdma_name,
                            qp->qp_num,
                            (hs_ip >> 24) & 0xFF, (hs_ip >> 16) & 0xFF,
                            (hs_ip >> 8) & 0xFF, hs_ip & 0xFF, hs_port);

                    if (mesh_rdma_send_handshake(hs_ip, hs_port,
                                                  &local_info, &remote_info) != 0) {
                        fprintf(stderr, "[mesh-mpi] rank %d: handshake to rank %d failed\n",
                                g_mpi.rank, peer);
                        ibv_destroy_qp(qp);
                        ibv_destroy_cq(cq);
                        continue;
                    }

                    /* Connect QP: INIT → RTR → RTS */
                    if (mesh_rdma_connect_qp(qp, local_nic, &remote_info) != 0) {
                        fprintf(stderr, "[mesh-mpi] rank %d: QP connect to rank %d failed\n",
                                g_mpi.rank, peer);
                        ibv_destroy_qp(qp);
                        ibv_destroy_cq(cq);
                        continue;
                    }

                    /* Verify RTS */
                    {
                        struct ibv_qp_attr qchk;
                        struct ibv_qp_init_attr qi;
                        memset(&qchk, 0, sizeof(qchk));
                        if (ibv_query_qp(qp, &qchk, IBV_QP_STATE, &qi) == 0 &&
                            qchk.qp_state != IBV_QPS_RTS) {
                            fprintf(stderr, "[mesh-mpi] rank %d: QP to rank %d NOT RTS (state=%d)\n",
                                    g_mpi.rank, peer, qchk.qp_state);
                            ibv_destroy_qp(qp);
                            ibv_destroy_cq(cq);
                            continue;
                        }
                    }

                    /* Build connection object */
                    mesh_rdma_conn_t *conn = calloc(1, sizeof(*conn));
                    conn->nic = local_nic;
                    conn->qp = qp;
                    conn->cq = cq;
                    conn->remote_qp_num = ntohl(remote_info.qp_num);
                    memcpy(&conn->remote_gid, remote_info.gid, 16);
                    conn->connected = 1;
                    p->conn = conn;

                    fprintf(stderr, "[mesh-mpi] rank %d: connected to rank %d "
                            "(local QP %u on NIC %s, remote QP %u)\n",
                            g_mpi.rank, peer, qp->qp_num, local_nic->rdma_name,
                            conn->remote_qp_num);
                }

                /* Register send/recv buffers on the SAME NIC as the connection's QP.
                 * Derive nic_idx from the connection to guarantee PD match. */
                p->nic_idx = (int)(p->conn->nic - g_mpi.ctx->nics);
                int num_slots = 16;
                size_t slot_size = 4 * 1024 * 1024;
                size_t pool_size = num_slots * slot_size;
                p->send_pool = calloc(1, pool_size);
                p->recv_pool = calloc(1, pool_size);
                if (!p->send_pool || !p->recv_pool) {
                    fprintf(stderr, "[mesh-mpi] rank %d: buffer alloc failed\n", g_mpi.rank);
                    continue;
                }
                if (mesh_rdma_reg_mr(g_mpi.ctx, p->nic_idx, p->send_pool,
                                     pool_size, &p->send_mr) != 0) {
                    fprintf(stderr, "[mesh-mpi] rank %d: send MR reg failed (nic=%d)\n",
                            g_mpi.rank, p->nic_idx);
                    continue;
                }
                if (mesh_rdma_reg_mr(g_mpi.ctx, p->nic_idx, p->recv_pool,
                                     pool_size, &p->recv_mr) != 0) {
                    fprintf(stderr, "[mesh-mpi] rank %d: recv MR reg failed (nic=%d)\n",
                            g_mpi.rank, p->nic_idx);
                    continue;
                }
                for (int r = 0; r < num_slots; r++) {
                    int rc = mesh_rdma_post_recv(p->conn,
                                        p->recv_pool + r * slot_size,
                                        slot_size,
                                        p->recv_mr->mr->lkey, r);
                    if (rc != 0) {
                        fprintf(stderr, "[mesh-mpi] rank %d: post_recv slot %d failed "
                                "(rc=%d, nic=%d, lkey=0x%x) — PD mismatch?\n",
                                g_mpi.rank, r, rc, p->nic_idx, p->recv_mr->mr->lkey);
                    }
                }
                fprintf(stderr, "[mesh-mpi] rank %d: buffers registered (%d x %zu)\n",
                        g_mpi.rank, num_slots, slot_size);
                p->direct = 1;
                p->send_head = 0;
                connected = 1;

                fprintf(stderr, "[mesh-mpi] rank %d: RDMA connected to rank %d\n",
                        g_mpi.rank, peer);
            }
        }

        if (!connected) {
            fprintf(stderr, "[mesh-mpi] rank %d: WARNING no direct link to ring neighbor %d\n",
                    g_mpi.rank, peer);
        }

        /* Sync between phases — give accepting side time to re-enter listen */
        if (n == 0) {
            fprintf(stderr, "[mesh-mpi] rank %d: phase 1 complete, syncing...\n", g_mpi.rank);
            usleep(500000); /* 500ms */
        }
    }

    /* Phase 4: Compute relay routes for all ranks */
    }
    mesh_mpi_compute_ring_routes();

    /* TCP barrier — ensure all ranks have finished connections before proceeding */
    {
        char ctrl_ip[64];
        int ctrl_port = 19900;
        char *s = getenv("MESH_MPI_CTRL_ADDR");
        strncpy(ctrl_ip, s, sizeof(ctrl_ip)-1);
        char *c = strchr(ctrl_ip, ':');
        if (c) { *c = 0; ctrl_port = atoi(c+1); }

        if (g_mpi.rank == 0) {
            int lsock = socket(AF_INET, SOCK_STREAM, 0);
            int opt = 1;
            setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            struct sockaddr_in a = {0};
            a.sin_family = AF_INET;
            a.sin_port = htons(ctrl_port + 1);
            a.sin_addr.s_addr = htonl(INADDR_ANY);
            bind(lsock, (struct sockaddr*)&a, sizeof(a));
            listen(lsock, 32);
            /* Collect checkins */
            for (int i = 0; i < g_mpi.size - 1; i++) {
                int cs = accept(lsock, NULL, NULL);
                char b; recv(cs, &b, 1, 0); close(cs);
            }
            /* Release everyone */
            close(lsock);
            lsock = socket(AF_INET, SOCK_STREAM, 0);
            setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
            a.sin_port = htons(ctrl_port + 2);
            bind(lsock, (struct sockaddr*)&a, sizeof(a));
            listen(lsock, 32);
            /* Tell everyone to go */
            for (int i = 0; i < g_mpi.size - 1; i++) {
                int cs = accept(lsock, NULL, NULL);
                char b = 1; send(cs, &b, 1, 0); close(cs);
            }
            close(lsock);
        } else {
            usleep(100000); /* let rank 0 start listening */
            /* Check in */
            int s1 = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in a = {0};
            a.sin_family = AF_INET;
            a.sin_port = htons(ctrl_port + 1);
            inet_pton(AF_INET, ctrl_ip, &a.sin_addr);
            for (int r = 0; r < 50; r++) {
                if (connect(s1, (struct sockaddr*)&a, sizeof(a)) == 0) break;
                close(s1); s1 = socket(AF_INET, SOCK_STREAM, 0);
                usleep(100000);
            }
            char b = 1; send(s1, &b, 1, 0); close(s1);
            /* Wait for release */
            usleep(100000);
            int s2 = socket(AF_INET, SOCK_STREAM, 0);
            a.sin_port = htons(ctrl_port + 2);
            for (int r = 0; r < 50; r++) {
                if (connect(s2, (struct sockaddr*)&a, sizeof(a)) == 0) break;
                close(s2); s2 = socket(AF_INET, SOCK_STREAM, 0);
                usleep(100000);
            }
            recv(s2, &b, 1, 0); close(s2);
        }
        fprintf(stderr, "[mesh-mpi] rank %d: bootstrap barrier done\n", g_mpi.rank);
    }

    /* free(all_msgs); -- leaked intentionally for debug */
    return 0;
}

/* ============================================================
 * Ring routing: shortest path on the ring
 * ============================================================ */

int mesh_mpi_compute_ring_routes(void) {
    /* Determine which neighbors we can actually reach */
    int have_next = g_mpi.peers[g_mpi.ring_next].direct;
    int have_prev = g_mpi.peers[g_mpi.ring_prev].direct;

    for (int dest = 0; dest < g_mpi.size; dest++) {
        if (dest == g_mpi.rank) {
            g_mpi.ring_route[dest] = -1; /* self */
            continue;
        }

        if (have_next && have_prev) {
            /* Both neighbors connected — use shortest path */
            int cw = (dest - g_mpi.rank + g_mpi.size) % g_mpi.size;
            int ccw = g_mpi.size - cw;
            if (cw <= ccw)
                g_mpi.ring_route[dest] = g_mpi.ring_next;
            else
                g_mpi.ring_route[dest] = g_mpi.ring_prev;
        } else if (have_next) {
            g_mpi.ring_route[dest] = g_mpi.ring_next;
        } else if (have_prev) {
            g_mpi.ring_route[dest] = g_mpi.ring_prev;
        } else {
            g_mpi.ring_route[dest] = -1; /* isolated */
        }

        fprintf(stderr, "[mesh-mpi] rank %d: route to %d via %d%s\n",
                g_mpi.rank, dest, g_mpi.ring_route[dest],
                g_mpi.ring_route[dest] == dest ? " (direct)" : " (relay)");
    }
    return 0;
}

/* ============================================================
 * MPI_Init / MPI_Init_thread
 * ============================================================ */

int MPI_Init(int *argc, char ***argv) {
    return MPI_Init_thread(argc, argv, MPI_THREAD_SINGLE, &g_mpi.thread_level);
}

int MPI_Init_thread(int *argc, char ***argv, int required, int *provided) {
    (void)argc; (void)argv;

    if (g_mpi.initialized) return MPI_SUCCESS;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    g_mpi.t_start = ts.tv_sec + ts.tv_nsec * 1e-9;

    pthread_mutex_init(&g_mpi.req_lock, NULL);

    /* Bootstrap: discover peers, exchange handles, connect */
    if (mesh_mpi_bootstrap() != 0) {
        fprintf(stderr, "[mesh-mpi] Bootstrap failed\n");
        return MPI_ERR_OTHER;
    }

    /* Set up MPI_COMM_WORLD */
    g_comm_world.id = 0;
    g_comm_world.size = g_mpi.size;
    g_comm_world.rank = g_mpi.rank;
    g_comm_world.ranks = g_comm_world_ranks;
    for (int i = 0; i < g_mpi.size; i++)
        g_comm_world_ranks[i] = i;

    /* Set up MPI_COMM_SELF */
    static int self_rank_list[1];
    self_rank_list[0] = g_mpi.rank;
    g_comm_self.id = 1;
    g_comm_self.size = 1;
    g_comm_self.rank = 0;
    g_comm_self.ranks = self_rank_list;

    g_mpi.num_comms = 2;
    g_mpi.comms[0] = g_comm_world;
    g_mpi.comms[1] = g_comm_self;

    g_mpi.initialized = 1;
    fprintf(stderr, "[mesh-mpi] rank %d: about to return from Init\n", g_mpi.rank);
    g_mpi.thread_level = required;
    *provided = required; /* we support all thread levels */

    fprintf(stderr, "[mesh-mpi] rank %d: MPI_Init complete (world=%d)\n",
            g_mpi.rank, g_mpi.size);

    /* No barrier here — AMReX does its own synchronization */

    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Finalize
 * ============================================================ */

int MPI_Finalize(void) {
    if (!g_mpi.initialized) return MPI_SUCCESS;

    MPI_Barrier(MPI_COMM_WORLD);

    /* Close all connections */
    for (int i = 0; i < g_mpi.size; i++) {
        mesh_mpi_peer_t *p = &g_mpi.peers[i];
        if (!p->direct) continue;
        if (p->send_mr) mesh_rdma_dereg_mr(p->send_mr);
        if (p->recv_mr) mesh_rdma_dereg_mr(p->recv_mr);
        free(p->send_pool);
        free(p->recv_pool);
        if (p->conn) mesh_rdma_close(p->conn);
    }

    if (g_mpi.ctx) mesh_rdma_destroy(g_mpi.ctx);

    pthread_mutex_destroy(&g_mpi.req_lock);
    g_mpi.initialized = 0;

    fprintf(stderr, "[mesh-mpi] rank %d: MPI_Finalize complete\n", g_mpi.rank);
    return MPI_SUCCESS;
}

/* ============================================================
 * Simple query functions
 * ============================================================ */

int MPI_Comm_rank(MPI_Comm comm, int *rank) {
    if (!comm) return MPI_ERR_COMM;
    *rank = comm->rank;
    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int *size) {
    if (!comm) return MPI_ERR_COMM;
    *size = comm->size;
    return MPI_SUCCESS;
}

int MPI_Initialized(int *flag) {
    *flag = g_mpi.initialized;
    return MPI_SUCCESS;
}

int MPI_Finalized(int *flag) {
    *flag = !g_mpi.initialized;
    return MPI_SUCCESS;
}

int MPI_Query_thread(int *provided) {
    *provided = g_mpi.thread_level;
    return MPI_SUCCESS;
}

int MPI_Get_processor_name(char *name, int *resultlen) {
    gethostname(name, MPI_MAX_PROCESSOR_NAME);
    *resultlen = strlen(name);
    return MPI_SUCCESS;
}

double MPI_Wtime(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec + ts.tv_nsec * 1e-9) - g_mpi.t_start;
}

int MPI_Abort(MPI_Comm comm, int errorcode) {
    (void)comm;
    fprintf(stderr, "[mesh-mpi] rank %d: MPI_Abort(%d)\n", g_mpi.rank, errorcode);
    _exit(errorcode);
    return MPI_SUCCESS; /* unreachable */
}

int MPI_Error_string(int errorcode, char *string, int *resultlen) {
    snprintf(string, MPI_MAX_ERROR_STRING, "MPI error %d", errorcode);
    *resultlen = strlen(string);
    return MPI_SUCCESS;
}

/* ============================================================
 * Communicator operations (bookkeeping only)
 * ============================================================ */

int MPI_Comm_dup(MPI_Comm comm, MPI_Comm *newcomm) {
    fprintf(stderr, "[mesh-mpi] rank %d: Comm_dup\n", g_mpi.rank); fflush(stderr);
    if (!comm) return MPI_ERR_COMM;
    struct mesh_mpi_comm_t *nc = &g_mpi.comms[g_mpi.num_comms];
    nc->id = g_mpi.num_comms;
    nc->size = comm->size;
    nc->rank = comm->rank;
    nc->ranks = malloc(comm->size * sizeof(int));
    memcpy(nc->ranks, comm->ranks, comm->size * sizeof(int));
    *newcomm = nc;
    g_mpi.num_comms++;
    return MPI_SUCCESS;
}

int MPI_Comm_free(MPI_Comm *comm) {
    if (!comm || !*comm) return MPI_ERR_COMM;
    /* Don't free WORLD or SELF */
    if (*comm == MPI_COMM_WORLD || *comm == MPI_COMM_SELF) {
        *comm = MPI_COMM_NULL;
        return MPI_SUCCESS;
    }
    if ((*comm)->ranks) free((*comm)->ranks);
    *comm = MPI_COMM_NULL;
    return MPI_SUCCESS;
}

int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm *newcomm) {
    fprintf(stderr, "[mesh-mpi] rank %d: ENTER Comm_split color=%d\n", g_mpi.rank, color);
    /* Simplified: gather (color, key, rank) from all, sort, build new comm */
    /* For now, if color == MPI_UNDEFINED, return COMM_NULL */
    if (color < 0) {
        *newcomm = MPI_COMM_NULL;
        return MPI_SUCCESS;
    }
    /* TODO: implement properly with allgather of colors/keys */
    return MPI_Comm_dup(comm, newcomm);
}

int MPI_Comm_split_type(MPI_Comm comm, int split_type, int key,
                        MPI_Info info, MPI_Comm *newcomm) {
    (void)info; (void)key;
    fprintf(stderr, "[mesh-mpi] rank %d: Comm_split_type called (type=%d)\n", g_mpi.rank, split_type);
    if (split_type == MPI_COMM_TYPE_SHARED) {
        /* Return a communicator with just this rank (each node has 1 process) */
        struct mesh_mpi_comm_t *sc = &g_mpi.comms[g_mpi.num_comms];
        sc->id = g_mpi.num_comms++;
        sc->size = 1;
        sc->rank = 0;
        sc->ranks = malloc(sizeof(int));
        sc->ranks[0] = g_mpi.rank;
        *newcomm = sc;
        return MPI_SUCCESS;
    }
    return MPI_Comm_dup(comm, newcomm);
}

int MPI_Comm_create(MPI_Comm comm, MPI_Group group, MPI_Comm *newcomm) {
    (void)comm; (void)group;
    *newcomm = MPI_COMM_NULL;
    return MPI_SUCCESS;
}

int MPI_Comm_group(MPI_Comm comm, MPI_Group *group) {
    /* Return comm pointer as group — simplified */
    *group = (MPI_Group)comm;
    return MPI_SUCCESS;
}

int MPI_Group_incl(MPI_Group group, int n, const int ranks[], MPI_Group *newgroup) {
    (void)group; (void)n; (void)ranks;
    *newgroup = MPI_GROUP_EMPTY;
    return MPI_SUCCESS;
}

int MPI_Group_translate_ranks(MPI_Group group1, int n, const int ranks1[],
                              MPI_Group group2, int ranks2[]) {
    (void)group1; (void)group2;
    for (int i = 0; i < n; i++) ranks2[i] = ranks1[i];
    return MPI_SUCCESS;
}

int MPI_Group_free(MPI_Group *group) {
    *group = MPI_GROUP_EMPTY;
    return MPI_SUCCESS;
}

int MPI_Comm_get_attr(MPI_Comm comm, int keyval, void *attr, int *flag) {
    (void)comm;
    if (keyval == MPI_TAG_UB) {
        static int tag_ub = 32767;
        *(int **)attr = &tag_ub;
        *flag = 1;
    } else if (keyval == MPI_WTIME_IS_GLOBAL) {
        static int wtime_global = 0;
        *(int **)attr = &wtime_global;
        *flag = 1;
    } else {
        *flag = 0;
    }
    return MPI_SUCCESS;
}
