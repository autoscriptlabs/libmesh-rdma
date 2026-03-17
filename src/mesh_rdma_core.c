/*
 * libmesh-rdma: Universal RDMA for Consumer Hardware
 * MIT License
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <dirent.h>
#include <poll.h>

#include "mesh_rdma.h"

/* ============================================================
 * Logging
 * ============================================================ */

static void default_log(int level, const char *fmt, ...) {
    if (level > MESH_RDMA_LOG_WARN) return;
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[mesh-rdma] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

#define LOG(ctx, level, fmt, ...) \
    do { \
        mesh_rdma_log_fn _fn = (ctx) ? (ctx)->log_fn : default_log; \
        if (_fn && level <= ((ctx) ? (ctx)->debug_level : MESH_RDMA_LOG_WARN)) \
            _fn(level, fmt, ##__VA_ARGS__); \
    } while(0)

#define LOG_ERR(ctx, fmt, ...)  LOG(ctx, MESH_RDMA_LOG_ERROR, fmt, ##__VA_ARGS__)
#define LOG_WARN(ctx, fmt, ...) LOG(ctx, MESH_RDMA_LOG_WARN, fmt, ##__VA_ARGS__)
#define LOG_INFO(ctx, fmt, ...) LOG(ctx, MESH_RDMA_LOG_INFO, fmt, ##__VA_ARGS__)
#define LOG_DBG(ctx, fmt, ...)  LOG(ctx, MESH_RDMA_LOG_DEBUG, fmt, ##__VA_ARGS__)

/* ============================================================
 * IP / Interface Utilities
 * ============================================================ */

static uint32_t ip_to_uint(const char *ip_str) {
    struct in_addr addr;
    if (inet_pton(AF_INET, ip_str, &addr) != 1) return 0;
    return ntohl(addr.s_addr);
}

static void uint_to_ip(uint32_t ip, char *buf, size_t len) {
    struct in_addr addr;
    addr.s_addr = htonl(ip);
    inet_ntop(AF_INET, &addr, buf, len);
}

static int get_interface_ip(const char *if_name, uint32_t *ip, uint32_t *mask) {
    struct ifaddrs *ifaddr, *ifa;
    int found = 0;

    if (getifaddrs(&ifaddr) == -1) return -1;

    for (ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        if (strcmp(ifa->ifa_name, if_name) != 0) continue;

        struct sockaddr_in *a = (struct sockaddr_in *)ifa->ifa_addr;
        struct sockaddr_in *m = (struct sockaddr_in *)ifa->ifa_netmask;
        *ip = ntohl(a->sin_addr.s_addr);
        *mask = ntohl(m->sin_addr.s_addr);
        found = 1;
        break;
    }
    freeifaddrs(ifaddr);
    return found ? 0 : -1;
}

static const char *find_netdev_for_rdma(const char *rdma_dev) {
    static char netdev[64];
    char path[256];
    DIR *dir;
    struct dirent *entry;

    snprintf(path, sizeof(path), "/sys/class/infiniband/%s/device/net", rdma_dev);
    dir = opendir(path);
    if (!dir) return NULL;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') {
            strncpy(netdev, entry->d_name, sizeof(netdev) - 1);
            netdev[sizeof(netdev) - 1] = '\0';
            closedir(dir);
            return netdev;
        }
    }
    closedir(dir);
    return NULL;
}

/* ============================================================
 * GID Utilities — the heart of the NVIDIA bypass
 * ============================================================ */

int mesh_rdma_gid_is_ipv4_mapped(const union ibv_gid *gid) {
    if (!gid) return 0;
    for (int i = 0; i < 10; i++) {
        if (gid->raw[i] != 0) return 0;
    }
    if (gid->raw[10] != 0xFF || gid->raw[11] != 0xFF) return 0;
    if (gid->raw[12] == 0 && gid->raw[13] == 0 &&
        gid->raw[14] == 0 && gid->raw[15] == 0) return 0;
    return 1;
}

int mesh_rdma_gid_is_link_local(const union ibv_gid *gid) {
    if (!gid) return 0;
    return (gid->raw[0] == 0xFE && gid->raw[1] == 0x80);
}

uint32_t mesh_rdma_gid_to_ipv4(const union ibv_gid *gid) {
    if (!mesh_rdma_gid_is_ipv4_mapped(gid)) return 0;
    return ((uint32_t)gid->raw[12] << 24) |
           ((uint32_t)gid->raw[13] << 16) |
           ((uint32_t)gid->raw[14] << 8) |
           ((uint32_t)gid->raw[15]);
}

int mesh_rdma_find_ipv4_gid_index(struct ibv_context *context,
                                   int port_num,
                                   uint32_t expected_ip) {
    union ibv_gid gid;
    int best_index = -1;
    struct ibv_port_attr port_attr;

    if (!context) return -1;
    if (ibv_query_port(context, port_num, &port_attr) != 0) return -1;

    int max_gids = port_attr.gid_tbl_len;
    if (max_gids > 16) max_gids = 16;

    for (int i = 0; i < max_gids; i++) {
        if (ibv_query_gid(context, port_num, i, &gid) != 0) continue;

        /* Skip all-zero GIDs */
        int all_zero = 1;
        for (int j = 0; j < 16; j++) {
            if (gid.raw[j] != 0) { all_zero = 0; break; }
        }
        if (all_zero) continue;

        if (mesh_rdma_gid_is_ipv4_mapped(&gid)) {
            uint32_t gid_ip = mesh_rdma_gid_to_ipv4(&gid);
            if (expected_ip != 0 && gid_ip == expected_ip) {
                return i;
            }
            if (best_index < 0) {
                best_index = i;
            }
        }
    }
    return best_index;
}

/* ============================================================
 * NIC Setup
 * ============================================================ */

static int setup_nic(mesh_rdma_ctx_t *ctx, mesh_rdma_nic_t *nic, struct ibv_device *device) {
    struct ibv_device_attr dev_attr;
    struct ibv_port_attr port_attr;

    nic->context = ibv_open_device(device);
    if (!nic->context) {
        LOG_WARN(ctx, "Failed to open device %s", ibv_get_device_name(device));
        return -1;
    }

    strncpy(nic->rdma_name, ibv_get_device_name(device), sizeof(nic->rdma_name) - 1);

    /* Find network interface */
    const char *netdev = find_netdev_for_rdma(nic->rdma_name);
    if (netdev) {
        strncpy(nic->net_name, netdev, sizeof(nic->net_name) - 1);
        if (get_interface_ip(nic->net_name, &nic->ip_addr, &nic->netmask) == 0) {
            nic->subnet = nic->ip_addr & nic->netmask;
            char ip_str[INET_ADDRSTRLEN];
            uint_to_ip(nic->ip_addr, ip_str, sizeof(ip_str));
            LOG_INFO(ctx, "NIC %s (%s): IP=%s", nic->rdma_name, nic->net_name, ip_str);
        }
    }

    if (ibv_query_device(nic->context, &dev_attr)) {
        LOG_WARN(ctx, "Failed to query device %s", nic->rdma_name);
        ibv_close_device(nic->context);
        return -1;
    }

    nic->max_qp = dev_attr.max_qp;
    nic->max_mr = dev_attr.max_mr;

    nic->port_num = 1;
    if (ibv_query_port(nic->context, nic->port_num, &port_attr)) {
        LOG_WARN(ctx, "Failed to query port for %s", nic->rdma_name);
        ibv_close_device(nic->context);
        return -1;
    }
    nic->active_mtu = port_attr.active_mtu;

    /* Allocate protection domain */
    nic->pd = ibv_alloc_pd(nic->context);
    if (!nic->pd) {
        LOG_WARN(ctx, "Failed to allocate PD for %s", nic->rdma_name);
        ibv_close_device(nic->context);
        return -1;
    }

    /* Auto-discover GID index — the NVIDIA bypass */
    int auto_gid = mesh_rdma_find_ipv4_gid_index(nic->context, nic->port_num, nic->ip_addr);
    if (auto_gid >= 0) {
        nic->gid_index = auto_gid;
        LOG_INFO(ctx, "NIC %s: auto-discovered IPv4-mapped GID at index %d",
                 nic->rdma_name, nic->gid_index);
    } else if (ctx && ctx->gid_index_override >= 0) {
        nic->gid_index = ctx->gid_index_override;
        LOG_WARN(ctx, "NIC %s: no IPv4-mapped GID found, using override %d",
                 nic->rdma_name, nic->gid_index);
    } else {
        nic->gid_index = 3; /* default fallback */
        LOG_WARN(ctx, "NIC %s: no IPv4-mapped GID found, using default 3", nic->rdma_name);
    }

    /* Cache the GID */
    if (ibv_query_gid(nic->context, nic->port_num, nic->gid_index, &nic->gid) != 0) {
        LOG_WARN(ctx, "Failed to query GID for %s", nic->rdma_name);
    }

    /* Validate GID type */
    if (!mesh_rdma_gid_is_ipv4_mapped(&nic->gid)) {
        LOG_WARN(ctx, "NIC %s: GID at index %d is NOT IPv4-mapped — connections may fail",
                 nic->rdma_name, nic->gid_index);
    }

    /* Get link speed */
    char speed_path[256];
    snprintf(speed_path, sizeof(speed_path),
             "/sys/class/infiniband/%s/ports/%d/rate", nic->rdma_name, nic->port_num);
    FILE *f = fopen(speed_path, "r");
    if (f) {
        int speed;
        if (fscanf(f, "%d", &speed) == 1) {
            nic->link_speed_mbps = speed * 1000; /* Gbps to Mbps */
        }
        fclose(f);
    }

    LOG_INFO(ctx, "Initialized NIC %s: max_qp=%d, gid_index=%d, speed=%d Mbps",
             nic->rdma_name, nic->max_qp, nic->gid_index, nic->link_speed_mbps);

    return 0;
}

/* ============================================================
 * Context Init / Destroy
 * ============================================================ */

int mesh_rdma_init(mesh_rdma_ctx_t **ctx_out) {
    mesh_rdma_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return -1;

    ctx->log_fn = default_log;
    ctx->gid_index_override = -1;
    ctx->timeout_ms = 5000;
    ctx->retry_count = 3;

    /* Read config from environment */
    const char *env;
    if ((env = getenv("MESH_RDMA_GID_INDEX")))
        ctx->gid_index_override = atoi(env);
    if ((env = getenv("MESH_RDMA_DEBUG")))
        ctx->debug_level = atoi(env);
    if ((env = getenv("MESH_RDMA_TIMEOUT_MS")))
        ctx->timeout_ms = atoi(env);
    if ((env = getenv("MESH_RDMA_RETRY_COUNT")))
        ctx->retry_count = atoi(env);

    /* Discover RDMA devices */
    int num_devices;
    struct ibv_device **device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices == 0) {
        LOG_ERR(ctx, "No RDMA devices found");
        free(ctx);
        return -1;
    }

    LOG_INFO(ctx, "Found %d RDMA device(s)", num_devices);

    for (int i = 0; i < num_devices && ctx->num_nics < MESH_RDMA_MAX_NICS; i++) {
        if (setup_nic(ctx, &ctx->nics[ctx->num_nics], device_list[i]) == 0) {
            ctx->num_nics++;
        }
    }

    ibv_free_device_list(device_list);

    if (ctx->num_nics == 0) {
        LOG_ERR(ctx, "No usable RDMA NICs found");
        free(ctx);
        return -1;
    }

    LOG_INFO(ctx, "Initialized %d NIC(s)", ctx->num_nics);
    ctx->initialized = 1;
    *ctx_out = ctx;
    return 0;
}

void mesh_rdma_destroy(mesh_rdma_ctx_t *ctx) {
    if (!ctx) return;
    for (int i = 0; i < ctx->num_nics; i++) {
        mesh_rdma_nic_t *nic = &ctx->nics[i];
        if (nic->pd) ibv_dealloc_pd(nic->pd);
        if (nic->context) ibv_close_device(nic->context);
    }
    free(ctx);
}

void mesh_rdma_set_log(mesh_rdma_ctx_t *ctx, mesh_rdma_log_fn fn) {
    if (ctx) ctx->log_fn = fn;
}

/* ============================================================
 * NIC Query
 * ============================================================ */

int mesh_rdma_num_nics(mesh_rdma_ctx_t *ctx) {
    return ctx ? ctx->num_nics : 0;
}

mesh_rdma_nic_t *mesh_rdma_get_nic(mesh_rdma_ctx_t *ctx, int idx) {
    if (!ctx || idx < 0 || idx >= ctx->num_nics) return NULL;
    return &ctx->nics[idx];
}

mesh_rdma_nic_t *mesh_rdma_find_nic_for_ip(mesh_rdma_ctx_t *ctx, uint32_t peer_ip) {
    if (!ctx) return NULL;
    for (int i = 0; i < ctx->num_nics; i++) {
        mesh_rdma_nic_t *nic = &ctx->nics[i];
        /* Skip NICs with no IP — zero netmask matches everything */
        if (nic->ip_addr == 0) continue;
        if ((peer_ip & nic->netmask) == nic->subnet) {
            return nic;
        }
    }
    return NULL;
}

/* ============================================================
 * QP State Machine — INIT → RTR → RTS
 * This is the core that bypasses NVIDIA's firmware restrictions.
 * ============================================================ */

int mesh_rdma_create_qp(mesh_rdma_nic_t *nic,
                        struct ibv_qp **qp_out,
                        struct ibv_cq **cq_out) {
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_qp_init_attr qp_init_attr;

    cq = ibv_create_cq(nic->context, MESH_RDMA_CQ_DEPTH, NULL, NULL, 0);
    if (!cq) return -1;

    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_send_wr = MESH_RDMA_MAX_SEND_WR;
    qp_init_attr.cap.max_recv_wr = MESH_RDMA_MAX_RECV_WR;
    qp_init_attr.cap.max_send_sge = MESH_RDMA_MAX_SGE;
    qp_init_attr.cap.max_recv_sge = MESH_RDMA_MAX_SGE;
    qp_init_attr.cap.max_inline_data = MESH_RDMA_MAX_INLINE;

    qp = ibv_create_qp(nic->pd, &qp_init_attr);
    if (!qp) {
        ibv_destroy_cq(cq);
        return -1;
    }

    /* Transition to INIT */
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = nic->port_num;
    qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                               IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ;

    if (ibv_modify_qp(qp, &qp_attr,
                       IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                       IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
        ibv_destroy_qp(qp);
        ibv_destroy_cq(cq);
        return -1;
    }

    *qp_out = qp;
    *cq_out = cq;
    return 0;
}

int mesh_rdma_connect_qp(struct ibv_qp *qp,
                         mesh_rdma_nic_t *nic,
                         mesh_rdma_qp_info_t *remote) {
    struct ibv_qp_attr qp_attr;
    int ret;
    int max_retries = 5;
    int retry_delay_ms = 10;

    union ibv_gid remote_gid;
    memcpy(&remote_gid, remote->gid, 16);

    enum ibv_mtu path_mtu = remote->mtu ? remote->mtu : nic->active_mtu;

    /* RTR — Ready to Receive */
    for (int attempt = 0; attempt < max_retries; attempt++) {
        memset(&qp_attr, 0, sizeof(qp_attr));
        qp_attr.qp_state = IBV_QPS_RTR;
        qp_attr.path_mtu = path_mtu;
        qp_attr.dest_qp_num = ntohl(remote->qp_num);
        qp_attr.rq_psn = ntohl(remote->psn);
        qp_attr.max_dest_rd_atomic = 1;
        qp_attr.min_rnr_timer = 12;
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.dgid = remote_gid;
        qp_attr.ah_attr.grh.sgid_index = nic->gid_index;
        qp_attr.ah_attr.grh.hop_limit = 64;
        qp_attr.ah_attr.dlid = 0;
        qp_attr.ah_attr.sl = 0;
        qp_attr.ah_attr.src_path_bits = 0;
        qp_attr.ah_attr.port_num = nic->port_num;

        ret = ibv_modify_qp(qp, &qp_attr,
                IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);

        if (ret == 0) break;
        if (attempt < max_retries - 1) {
            fprintf(stderr, "[mesh-rdma] RTR attempt %d/%d failed: errno=%d (%s) dest_qp=%u gid_index=%d\n",
                    attempt+1, max_retries, errno, strerror(errno),
                    ntohl(remote->qp_num), nic->gid_index);
            usleep(retry_delay_ms * 1000);
            retry_delay_ms *= 2;
        } else {
            fprintf(stderr, "[mesh-rdma] RTR FAILED after %d attempts: errno=%d (%s) dest_qp=%u gid_index=%d\n",
                    max_retries, errno, strerror(errno),
                    ntohl(remote->qp_num), nic->gid_index);
            fprintf(stderr, "[mesh-rdma] Local NIC: %s, GID index: %d, MTU: %d\n",
                    nic->rdma_name, nic->gid_index, path_mtu);
            fprintf(stderr, "[mesh-rdma] Remote GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
                    remote->gid[0], remote->gid[1], remote->gid[2], remote->gid[3],
                    remote->gid[4], remote->gid[5], remote->gid[6], remote->gid[7],
                    remote->gid[8], remote->gid[9], remote->gid[10], remote->gid[11],
                    remote->gid[12], remote->gid[13], remote->gid[14], remote->gid[15]);
            return -1;
        }
    }

    /* RTS — Ready to Send */
    retry_delay_ms = 10;
    for (int attempt = 0; attempt < max_retries; attempt++) {
        memset(&qp_attr, 0, sizeof(qp_attr));
        qp_attr.qp_state = IBV_QPS_RTS;
        qp_attr.timeout = 18;       /* ~1s */
        qp_attr.retry_cnt = 7;
        qp_attr.rnr_retry = 7;
        qp_attr.sq_psn = 0;
        qp_attr.max_rd_atomic = 1;

        ret = ibv_modify_qp(qp, &qp_attr,
                IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);

        if (ret == 0) break;
        if (attempt < max_retries - 1) {
            fprintf(stderr, "[mesh-rdma] RTS attempt %d/%d failed: errno=%d (%s)\n",
                    attempt+1, max_retries, errno, strerror(errno));
            usleep(retry_delay_ms * 1000);
            retry_delay_ms *= 2;
        } else {
            fprintf(stderr, "[mesh-rdma] RTS FAILED after %d attempts: errno=%d (%s)\n",
                    max_retries, errno, strerror(errno));
            return -1;
        }
    }

    return 0;
}

/* ============================================================
 * TCP Handshake — Out-of-band QP info exchange
 * ============================================================ */

int mesh_rdma_create_handshake_socket(uint32_t bind_ip, uint16_t *port_out) {
    int sock;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    int opt = 1;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(bind_ip);
    addr.sin_port = 0;

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    if (listen(sock, 16) < 0) {
        close(sock);
        return -1;
    }
    if (getsockname(sock, (struct sockaddr *)&addr, &addrlen) < 0) {
        close(sock);
        return -1;
    }

    *port_out = ntohs(addr.sin_port);
    return sock;
}

int mesh_rdma_accept_handshake(int listen_sock,
                               mesh_rdma_qp_info_t *remote_info,
                               mesh_rdma_qp_info_t *local_info) {
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);

    int conn = accept(listen_sock, (struct sockaddr *)&addr, &addrlen);
    if (conn < 0) return -1;

    struct timeval tv = { .tv_sec = 10, .tv_usec = 0 };
    setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(conn, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    ssize_t n = recv(conn, remote_info, sizeof(*remote_info), MSG_WAITALL);
    if (n != sizeof(*remote_info)) { close(conn); return -1; }

    n = send(conn, local_info, sizeof(*local_info), 0);
    if (n != sizeof(*local_info)) { close(conn); return -1; }

    close(conn);
    return 0;
}

int mesh_rdma_send_handshake(uint32_t remote_ip, uint16_t remote_port,
                             mesh_rdma_qp_info_t *local_info,
                             mesh_rdma_qp_info_t *remote_info) {
    struct sockaddr_in addr;
    int sock;
    int connected = 0;
    int max_retries = 50;
    int retry_ms = 100;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(remote_ip);
    addr.sin_port = htons(remote_port);

    while (max_retries > 0 && !connected) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return -1;

        int flags = fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags | O_NONBLOCK);

        int ret = connect(sock, (struct sockaddr *)&addr, sizeof(addr));
        if (ret == 0) {
            connected = 1;
        } else if (errno == EINPROGRESS) {
            fd_set wfds;
            struct timeval tv = { .tv_sec = 0, .tv_usec = retry_ms * 1000 };
            FD_ZERO(&wfds);
            FD_SET(sock, &wfds);

            ret = select(sock + 1, NULL, &wfds, NULL, &tv);
            if (ret > 0) {
                int error = 0;
                socklen_t len = sizeof(error);
                getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len);
                if (error == 0) connected = 1;
                else { close(sock); max_retries--; }
            } else { close(sock); max_retries--; }
        } else { close(sock); max_retries--; usleep(retry_ms * 1000); }
    }

    if (!connected) return -1;

    /* Set blocking */
    {
        int flags = fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
    }

    struct timeval tv = { .tv_sec = 10, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    ssize_t n = send(sock, local_info, sizeof(*local_info), 0);
    if (n != sizeof(*local_info)) { close(sock); return -1; }

    n = recv(sock, remote_info, sizeof(*remote_info), MSG_WAITALL);
    if (n != sizeof(*remote_info)) { close(sock); return -1; }

    close(sock);
    return 0;
}

/* ============================================================
 * High-level Connection API
 * ============================================================ */

int mesh_rdma_listen(mesh_rdma_ctx_t *ctx, int nic_idx,
                     mesh_rdma_handle_t *handle_out,
                     void **listen_state) {
    if (!ctx || nic_idx < 0 || nic_idx >= ctx->num_nics) return -1;

    mesh_rdma_nic_t *nic = &ctx->nics[nic_idx];

    /* Create QP */
    struct ibv_qp *qp;
    struct ibv_cq *cq;
    if (mesh_rdma_create_qp(nic, &qp, &cq) != 0) {
        LOG_ERR(ctx, "Failed to create QP for listen");
        return -1;
    }

    /* Create handshake socket */
    uint16_t port;
    int sock = mesh_rdma_create_handshake_socket(INADDR_ANY, &port);
    if (sock < 0) {
        ibv_destroy_qp(qp);
        ibv_destroy_cq(cq);
        return -1;
    }

    /* Pack listen state */
    /* We allocate a simple struct to hold QP + CQ + socket + NIC */
    typedef struct {
        struct ibv_qp *qp;
        struct ibv_cq *cq;
        int sock;
        mesh_rdma_nic_t *nic;
    } listen_ctx_t;

    listen_ctx_t *lctx = calloc(1, sizeof(*lctx));
    lctx->qp = qp;
    lctx->cq = cq;
    lctx->sock = sock;
    lctx->nic = nic;

    /* Fill handle */
    memset(handle_out, 0, sizeof(*handle_out));
    handle_out->magic = 0x4D524D48;
    handle_out->handshake_port = port;
    handle_out->qp_num = qp->qp_num;
    handle_out->psn = 0;
    handle_out->gid_index = nic->gid_index;
    handle_out->mtu = nic->active_mtu;
    memcpy(&handle_out->gid, &nic->gid, sizeof(nic->gid));

    *listen_state = lctx;

    LOG_INFO(ctx, "Listening on port %d, QP %d", port, qp->qp_num);
    return 0;
}

int mesh_rdma_connect(mesh_rdma_ctx_t *ctx,
                      mesh_rdma_handle_t *remote_handle,
                      mesh_rdma_conn_t **conn_out) {
    if (!ctx || !remote_handle) return -1;

    /* Find NIC on same subnet as remote peer */
    union ibv_gid rgid;
    memcpy(&rgid, &remote_handle->gid, sizeof(rgid));
    uint32_t remote_ip = mesh_rdma_gid_to_ipv4(&rgid);
    if (remote_ip == 0 && remote_handle->addrs[0].ip != 0)
        remote_ip = ntohl(remote_handle->addrs[0].ip);

    mesh_rdma_nic_t *nic = mesh_rdma_find_nic_for_ip(ctx, remote_ip);
    if (!nic) {
        /* Fallback: first NIC with a valid IP */
        for (int i = 0; i < ctx->num_nics; i++) {
            if (ctx->nics[i].ip_addr != 0) { nic = &ctx->nics[i]; break; }
        }
    }
    if (!nic) {
        LOG_ERR(ctx, "No suitable NIC found for remote IP 0x%x", remote_ip);
        return -1;
    }
    fprintf(stderr, "[mesh-rdma] Connect: remote_ip=0x%x, selected NIC=%s (ip=0x%x)\n",
            remote_ip, nic->rdma_name, nic->ip_addr);

    /* Create local QP */
    struct ibv_qp *qp;
    struct ibv_cq *cq;
    if (mesh_rdma_create_qp(nic, &qp, &cq) != 0) {
        LOG_ERR(ctx, "Failed to create QP for connect");
        return -1;
    }

    /* Prepare local QP info */
    mesh_rdma_qp_info_t local_info, remote_info;
    memset(&local_info, 0, sizeof(local_info));
    local_info.qp_num = htonl(qp->qp_num);
    local_info.psn = htonl(0);
    memcpy(local_info.gid, &nic->gid, 16);
    local_info.gid_index = nic->gid_index;
    local_info.mtu = nic->active_mtu;

    /* remote_ip already extracted above for NIC selection */
    fprintf(stderr, "[mesh-rdma] Connect: sending local GID %02x%02x:...:%02x%02x, qp=%u, gid_idx=%d\n",
            local_info.gid[0], local_info.gid[1], local_info.gid[14], local_info.gid[15],
            ntohl(local_info.qp_num), local_info.gid_index);

    /* TCP handshake — use override IP if provided */
    uint32_t handshake_dest = remote_handle->handshake_ip ? remote_handle->handshake_ip : remote_ip;
    if (mesh_rdma_send_handshake(handshake_dest, remote_handle->handshake_port,
                                  &local_info, &remote_info) != 0) {
        LOG_ERR(ctx, "Handshake failed");
        ibv_destroy_qp(qp);
        ibv_destroy_cq(cq);
        return -1;
    }

    /* Connect QP */
    if (mesh_rdma_connect_qp(qp, nic, &remote_info) != 0) {
        LOG_ERR(ctx, "QP connect failed");
        ibv_destroy_qp(qp);
        ibv_destroy_cq(cq);
        return -1;
    }

    /* Verify QP reached RTS state */
    {
        struct ibv_qp_attr qp_check;
        struct ibv_qp_init_attr qp_init_check;
        memset(&qp_check, 0, sizeof(qp_check));
        if (ibv_query_qp(qp, &qp_check, IBV_QP_STATE, &qp_init_check) == 0) {
            if (qp_check.qp_state != IBV_QPS_RTS) {
                fprintf(stderr, "[mesh-rdma] CONNECT: QP %u NOT in RTS (state=%d), expected RTS=%d\n",
                        qp->qp_num, qp_check.qp_state, IBV_QPS_RTS);
                ibv_destroy_qp(qp);
                ibv_destroy_cq(cq);
                return -1;
            }
        }
    }

    /* Build connection */
    mesh_rdma_conn_t *conn = calloc(1, sizeof(*conn));
    conn->nic = nic;
    conn->qp = qp;
    conn->cq = cq;
    conn->remote_qp_num = ntohl(remote_info.qp_num);
    memcpy(&conn->remote_gid, remote_info.gid, 16);
    conn->connected = 1;

    *conn_out = conn;
    LOG_INFO(ctx, "Connected to remote QP %d (verified RTS)", conn->remote_qp_num);
    return 0;
}

int mesh_rdma_accept(mesh_rdma_ctx_t *ctx,
                     void *listen_state,
                     mesh_rdma_conn_t **conn_out) {
    typedef struct {
        struct ibv_qp *qp;
        struct ibv_cq *cq;
        int sock;
        mesh_rdma_nic_t *nic;
    } listen_ctx_t;

    listen_ctx_t *lctx = (listen_ctx_t *)listen_state;
    if (!lctx) return -1;

    /* Prepare local QP info */
    mesh_rdma_qp_info_t local_info, remote_info;
    memset(&local_info, 0, sizeof(local_info));
    local_info.qp_num = htonl(lctx->qp->qp_num);
    local_info.psn = htonl(0);
    memcpy(local_info.gid, &lctx->nic->gid, 16);
    local_info.gid_index = lctx->nic->gid_index;
    local_info.mtu = lctx->nic->active_mtu;

    /* Accept handshake */
    if (mesh_rdma_accept_handshake(lctx->sock, &remote_info, &local_info) != 0) {
        LOG_ERR(ctx, "Accept handshake failed");
        return -1;
    }

    fprintf(stderr, "[mesh-rdma] ACCEPT: local QP %u on NIC %s (PD=%p), "
            "remote QP %u, remote GID %02x%02x:...:%02x%02x\n",
            lctx->qp->qp_num, lctx->nic->rdma_name, (void *)lctx->nic->pd,
            ntohl(remote_info.qp_num),
            remote_info.gid[0], remote_info.gid[1],
            remote_info.gid[14], remote_info.gid[15]);

    /* Connect QP */
    if (mesh_rdma_connect_qp(lctx->qp, lctx->nic, &remote_info) != 0) {
        LOG_ERR(ctx, "QP connect on accept failed");
        return -1;
    }

    /* Verify QP reached RTS state */
    {
        struct ibv_qp_attr qp_check;
        struct ibv_qp_init_attr qp_init_check;
        memset(&qp_check, 0, sizeof(qp_check));
        if (ibv_query_qp(lctx->qp, &qp_check, IBV_QP_STATE, &qp_init_check) == 0) {
            if (qp_check.qp_state != IBV_QPS_RTS) {
                fprintf(stderr, "[mesh-rdma] ACCEPT: QP %u NOT in RTS (state=%d), expected RTS=%d\n",
                        lctx->qp->qp_num, qp_check.qp_state, IBV_QPS_RTS);
                return -1;
            }
            fprintf(stderr, "[mesh-rdma] ACCEPT: QP %u verified in RTS state\n",
                    lctx->qp->qp_num);
        }
    }

    /* Build connection */
    mesh_rdma_conn_t *conn = calloc(1, sizeof(*conn));
    conn->nic = lctx->nic;
    conn->qp = lctx->qp;
    conn->cq = lctx->cq;
    conn->remote_qp_num = ntohl(remote_info.qp_num);
    memcpy(&conn->remote_gid, remote_info.gid, 16);
    conn->connected = 1;

    *conn_out = conn;
    LOG_INFO(ctx, "Accepted connection from QP %d (verified RTS)", conn->remote_qp_num);
    return 0;
}

void mesh_rdma_close(mesh_rdma_conn_t *conn) {
    if (!conn) return;
    if (conn->qp) {
        struct ibv_qp_attr attr = { .qp_state = IBV_QPS_RESET };
        ibv_modify_qp(conn->qp, &attr, IBV_QP_STATE);
        ibv_destroy_qp(conn->qp);
    }
    if (conn->cq) ibv_destroy_cq(conn->cq);
    free(conn);
}

void mesh_rdma_close_listen(void *listen_state) {
    typedef struct {
        struct ibv_qp *qp;
        struct ibv_cq *cq;
        int sock;
        mesh_rdma_nic_t *nic;
    } listen_ctx_t;

    listen_ctx_t *lctx = (listen_ctx_t *)listen_state;
    if (!lctx) return;
    if (lctx->sock >= 0) close(lctx->sock);
    /* Don't destroy QP/CQ here — they might be owned by an accepted conn */
    free(lctx);
}

/* ============================================================
 * Memory Registration
 * ============================================================ */

int mesh_rdma_reg_mr(mesh_rdma_ctx_t *ctx, int nic_idx,
                     void *buf, size_t len,
                     mesh_rdma_mr_t **mr_out) {
    if (!ctx || nic_idx < 0 || nic_idx >= ctx->num_nics) return -1;

    mesh_rdma_nic_t *nic = &ctx->nics[nic_idx];
    mesh_rdma_mr_t *mr = calloc(1, sizeof(*mr));
    if (!mr) return -1;

    int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    mr->mr = ibv_reg_mr(nic->pd, buf, len, flags);
    if (!mr->mr) {
        LOG_ERR(ctx, "ibv_reg_mr failed: %s", strerror(errno));
        free(mr);
        return -1;
    }

    mr->addr = buf;
    mr->len = len;
    *mr_out = mr;
    return 0;
}

void mesh_rdma_dereg_mr(mesh_rdma_mr_t *mr) {
    if (!mr) return;
    if (mr->mr) ibv_dereg_mr(mr->mr);
    free(mr);
}

/* ============================================================
 * Data Transfer
 * ============================================================ */

int mesh_rdma_post_send(mesh_rdma_conn_t *conn,
                        void *buf, size_t len, uint32_t lkey,
                        uint64_t wr_id) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)buf,
        .length = len,
        .lkey = lkey,
    };
    struct ibv_send_wr wr, *bad_wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    return ibv_post_send(conn->qp, &wr, &bad_wr);
}

int mesh_rdma_post_recv(mesh_rdma_conn_t *conn,
                        void *buf, size_t len, uint32_t lkey,
                        uint64_t wr_id) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)buf,
        .length = len,
        .lkey = lkey,
    };
    struct ibv_recv_wr wr, *bad_wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(conn->qp, &wr, &bad_wr);
}

int mesh_rdma_poll(mesh_rdma_conn_t *conn, int *done, size_t *bytes) {
    struct ibv_wc wc;
    int ret = ibv_poll_cq(conn->cq, 1, &wc);

    if (ret < 0) return -1;
    if (ret == 0) { *done = 0; return 0; }

    if (wc.status != IBV_WC_SUCCESS) {
        *done = 1;
        return -1; /* Caller can inspect errno or add status output */
    }

    *done = 1;
    if (bytes) *bytes = wc.byte_len;
    return 0;
}
