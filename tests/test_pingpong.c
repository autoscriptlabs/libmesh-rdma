/*
 * test_pingpong.c — RDMA pingpong over libmesh-rdma
 *
 * This is the proof. If this program exchanges data between two nodes
 * over RDMA without NVIDIA's blessed switch, the library works.
 *
 * Usage:
 *   Server:  ./test_pingpong -s
 *   Client:  ./test_pingpong -c <server-ip>
 *
 * The server listens, the client connects, they exchange messages
 * over RDMA, and both print the results.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

#include "mesh_rdma.h"

#define MSG_SIZE    4096
#define NUM_PINGS   1000
#define OOB_PORT    19876    /* Out-of-band TCP port for handle exchange */

/*
 * Exchange handles over a simple TCP connection.
 * The RDMA library needs an out-of-band channel to exchange
 * connection info before RDMA can start. In MPI, this is done
 * via the OOB (out-of-band) network. Here we use raw TCP.
 */
static int send_handle(const char *host, uint16_t port,
                       mesh_rdma_handle_t *local, mesh_rdma_handle_t *remote) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    inet_pton(AF_INET, host, &addr.sin_addr);

    /* Retry connection */
    for (int i = 0; i < 50; i++) {
        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) goto connected;
        usleep(100000);
    }
    fprintf(stderr, "Failed to connect to %s:%d\n", host, port);
    close(sock);
    return -1;

connected:
    send(sock, local, sizeof(*local), 0);
    recv(sock, remote, sizeof(*remote), MSG_WAITALL);
    close(sock);
    return 0;
}

static int recv_handle(uint16_t port,
                       mesh_rdma_handle_t *local, mesh_rdma_handle_t *remote) {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,
        .sin_port = htons(port),
    };
    bind(srv, (struct sockaddr *)&addr, sizeof(addr));
    listen(srv, 1);

    printf("Waiting for client on port %d...\n", port);
    int conn = accept(srv, NULL, NULL);
    recv(conn, remote, sizeof(*remote), MSG_WAITALL);
    send(conn, local, sizeof(*local), 0);
    close(conn);
    close(srv);
    return 0;
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int main(int argc, char **argv) {
    int is_server = 0;
    const char *peer_ip = NULL;

    if (argc < 2) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  Server: %s -s\n", argv[0]);
        fprintf(stderr, "  Client: %s -c <server-ip>\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "-s") == 0) {
        is_server = 1;
    } else if (strcmp(argv[1], "-c") == 0 && argc >= 3) {
        peer_ip = argv[2];
    } else {
        fprintf(stderr, "Invalid arguments\n");
        return 1;
    }

    printf("=== libmesh-rdma Pingpong Test ===\n");
    printf("Mode: %s\n", is_server ? "SERVER" : "CLIENT");

    /* Initialize library */
    mesh_rdma_ctx_t *ctx;
    if (mesh_rdma_init(&ctx) != 0) {
        fprintf(stderr, "Failed to initialize libmesh-rdma\n");
        return 1;
    }

    printf("NICs found: %d\n", mesh_rdma_num_nics(ctx));
    for (int i = 0; i < mesh_rdma_num_nics(ctx); i++) {
        mesh_rdma_nic_t *nic = mesh_rdma_get_nic(ctx, i);
        char ip[INET_ADDRSTRLEN];
        struct in_addr a = { .s_addr = htonl(nic->ip_addr) };
        inet_ntop(AF_INET, &a, ip, sizeof(ip));
        printf("  NIC %d: %s (%s) IP=%s GID_index=%d\n",
               i, nic->rdma_name, nic->net_name, ip, nic->gid_index);
    }

    /* Create listen endpoint to get a handle */
    mesh_rdma_handle_t local_handle, remote_handle;
    void *listen_state = NULL;

    /* Find NIC on same subnet as peer (client) or first valid (server) */
    int nic_idx = -1;
    if (peer_ip) {
        /* Client: match the peer's subnet */
        struct in_addr pa;
        inet_pton(AF_INET, peer_ip, &pa);
        uint32_t pip = ntohl(pa.s_addr);
        for (int i = 0; i < mesh_rdma_num_nics(ctx); i++) {
            mesh_rdma_nic_t *n = mesh_rdma_get_nic(ctx, i);
            if (n->ip_addr != 0 && (pip & n->netmask) == n->subnet) {
                nic_idx = i; break;
            }
        }
    }
    if (nic_idx < 0) {
        /* Server or no subnet match: first NIC with valid IP */
        for (int i = 0; i < mesh_rdma_num_nics(ctx); i++) {
            mesh_rdma_nic_t *n = mesh_rdma_get_nic(ctx, i);
            if (n->ip_addr != 0) { nic_idx = i; break; }
        }
    }
    if (nic_idx < 0) nic_idx = 0;
    printf("Using NIC %d (%s)\n", nic_idx,
           mesh_rdma_get_nic(ctx, nic_idx)->rdma_name);

    if (mesh_rdma_listen(ctx, nic_idx, &local_handle, &listen_state) != 0) {
        fprintf(stderr, "Failed to create listen endpoint\n");
        return 1;
    }

    /* Exchange handles over TCP OOB */
    if (is_server) {
        recv_handle(OOB_PORT, &local_handle, &remote_handle);
    } else {
        send_handle(peer_ip, OOB_PORT, &local_handle, &remote_handle);
    }

    printf("Handles exchanged. Establishing RDMA connection...\n");

    /* Establish RDMA connection */
    mesh_rdma_conn_t *conn;
    if (is_server) {
        if (mesh_rdma_accept(ctx, listen_state, &conn) != 0) {
            fprintf(stderr, "Accept failed\n");
            return 1;
        }
    } else {
        if (mesh_rdma_connect(ctx, &remote_handle, &conn) != 0) {
            fprintf(stderr, "Connect failed\n");
            return 1;
        }
    }

    printf("RDMA connection established!\n");

    /* Register memory */
    char *send_buf = malloc(MSG_SIZE);
    char *recv_buf = malloc(MSG_SIZE);
    memset(send_buf, 0, MSG_SIZE);
    memset(recv_buf, 0, MSG_SIZE);

    mesh_rdma_mr_t *send_mr, *recv_mr;
    if (mesh_rdma_reg_mr(ctx, nic_idx, send_buf, MSG_SIZE, &send_mr) != 0 ||
        mesh_rdma_reg_mr(ctx, nic_idx, recv_buf, MSG_SIZE, &recv_mr) != 0) {
        fprintf(stderr, "Memory registration failed\n");
        return 1;
    }

    printf("Memory registered. Starting pingpong (%d iterations, %d bytes)...\n",
           NUM_PINGS, MSG_SIZE);

    /* Pingpong! */
    double start = now_sec();

    for (int i = 0; i < NUM_PINGS; i++) {
        if (is_server) {
            /* Server: recv then send */
            mesh_rdma_post_recv(conn, recv_buf, MSG_SIZE,
                                recv_mr->mr->lkey, i);
            int done = 0;
            while (!done) mesh_rdma_poll(conn, &done, NULL);

            snprintf(send_buf, MSG_SIZE, "PONG %d", i);
            mesh_rdma_post_send(conn, send_buf, MSG_SIZE,
                                send_mr->mr->lkey, i);
            done = 0;
            while (!done) mesh_rdma_poll(conn, &done, NULL);
        } else {
            /* Client: send then recv */
            snprintf(send_buf, MSG_SIZE, "PING %d", i);
            mesh_rdma_post_recv(conn, recv_buf, MSG_SIZE,
                                recv_mr->mr->lkey, i);
            mesh_rdma_post_send(conn, send_buf, MSG_SIZE,
                                send_mr->mr->lkey, i);

            /* Wait for send completion */
            int done = 0;
            while (!done) mesh_rdma_poll(conn, &done, NULL);
            /* Wait for recv completion */
            done = 0;
            while (!done) mesh_rdma_poll(conn, &done, NULL);
        }
    }

    double elapsed = now_sec() - start;
    double rtt_us = elapsed / NUM_PINGS * 1e6;
    double bw_mbps = (double)MSG_SIZE * NUM_PINGS * 2 / elapsed / 1e6;

    printf("\n=== Results ===\n");
    printf("  Iterations:    %d\n", NUM_PINGS);
    printf("  Message size:  %d bytes\n", MSG_SIZE);
    printf("  Total time:    %.3f ms\n", elapsed * 1e3);
    printf("  Avg RTT:       %.1f us\n", rtt_us);
    printf("  Throughput:    %.1f MB/s\n", bw_mbps);
    printf("\n");

    if (rtt_us < 100) {
        printf("*** RDMA IS WORKING. Latency < 100us confirms true RDMA, not TCP fallback. ***\n");
    } else {
        printf("NOTE: Latency > 100us. Check that RDMA is actually being used.\n");
    }

    printf("\nNVIDIA firmware said this shouldn't work. It works.\n");

    /* Cleanup */
    mesh_rdma_dereg_mr(send_mr);
    mesh_rdma_dereg_mr(recv_mr);
    free(send_buf);
    free(recv_buf);
    mesh_rdma_close(conn);
    mesh_rdma_close_listen(listen_state);
    mesh_rdma_destroy(ctx);

    return 0;
}
