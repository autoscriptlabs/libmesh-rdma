/*
 * test_bandwidth.c — Saturate the RDMA pipe
 *
 * Measures peak unidirectional bandwidth at various message sizes.
 * This is the number that proves the fat pipe is fat.
 *
 * Usage:
 *   Server:  ./test_bandwidth -s
 *   Client:  ./test_bandwidth -c <server-ip>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

#include "mesh_rdma.h"

#define OOB_PORT    19877
#define WARMUP      100
#define ITERATIONS  5000

/* Same OOB handle exchange as pingpong */
static int send_handle(const char *host, uint16_t port,
                       mesh_rdma_handle_t *local, mesh_rdma_handle_t *remote) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, host, &addr.sin_addr);
    for (int i = 0; i < 50; i++) {
        if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0) goto ok;
        usleep(100000);
    }
    close(sock); return -1;
ok:
    send(sock, local, sizeof(*local), 0);
    recv(sock, remote, sizeof(*remote), MSG_WAITALL);
    close(sock); return 0;
}

static int recv_handle(uint16_t port,
                       mesh_rdma_handle_t *local, mesh_rdma_handle_t *remote) {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_addr.s_addr = INADDR_ANY,
                                .sin_port = htons(port) };
    bind(srv, (struct sockaddr *)&addr, sizeof(addr));
    listen(srv, 1);
    printf("Waiting for client...\n");
    int conn = accept(srv, NULL, NULL);
    recv(conn, remote, sizeof(*remote), MSG_WAITALL);
    send(conn, local, sizeof(*local), 0);
    close(conn); close(srv); return 0;
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int main(int argc, char **argv) {
    int is_server = (argc >= 2 && strcmp(argv[1], "-s") == 0);
    const char *peer_ip = (argc >= 3) ? argv[2] : NULL;

    if (!is_server && !peer_ip) {
        fprintf(stderr, "Usage: %s -s | %s -c <ip>\n", argv[0], argv[0]);
        return 1;
    }

    printf("=== libmesh-rdma Bandwidth Test ===\n");

    mesh_rdma_ctx_t *ctx;
    mesh_rdma_init(&ctx);

    mesh_rdma_handle_t local_h, remote_h;
    void *listen_state;
    int nic_idx = 0;
    for (int i = 0; i < mesh_rdma_num_nics(ctx); i++) {
        mesh_rdma_nic_t *n = mesh_rdma_get_nic(ctx, i);
        if (n->ip_addr != 0) { nic_idx = i; break; }
    }
    mesh_rdma_listen(ctx, nic_idx, &local_h, &listen_state);

    if (is_server) recv_handle(OOB_PORT, &local_h, &remote_h);
    else           send_handle(peer_ip, OOB_PORT, &local_h, &remote_h);

    mesh_rdma_conn_t *conn;
    if (is_server) mesh_rdma_accept(ctx, listen_state, &conn);
    else           mesh_rdma_connect(ctx, &remote_h, &conn);

    printf("Connected. Running bandwidth test...\n\n");

    /* Test various message sizes */
    size_t sizes[] = { 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304 };
    int nsizes = sizeof(sizes) / sizeof(sizes[0]);

    size_t max_size = sizes[nsizes - 1];
    char *buf = malloc(max_size);
    memset(buf, 0xAA, max_size);

    mesh_rdma_mr_t *mr;
    mesh_rdma_reg_mr(ctx, nic_idx, buf, max_size, &mr);

    printf("%-12s %12s %12s %12s\n", "Size", "Iters", "Time (ms)", "BW (GB/s)");
    printf("%-12s %12s %12s %12s\n", "----", "-----", "--------", "--------");

    for (int s = 0; s < nsizes; s++) {
        size_t size = sizes[s];
        int iters = ITERATIONS;
        /* Scale down iterations for large messages */
        if (size >= 1048576) iters = 500;
        if (size >= 4194304) iters = 100;

        if (is_server) {
            /* Server: receive */
            for (int i = 0; i < WARMUP + iters; i++) {
                mesh_rdma_post_recv(conn, buf, size, mr->mr->lkey, i);
                int done = 0;
                while (!done) mesh_rdma_poll(conn, &done, NULL);
            }
        } else {
            /* Client: send and measure */
            /* Warmup */
            for (int i = 0; i < WARMUP; i++) {
                mesh_rdma_post_send(conn, buf, size, mr->mr->lkey, i);
                int done = 0;
                while (!done) mesh_rdma_poll(conn, &done, NULL);
            }

            /* Measured */
            double start = now_sec();
            for (int i = 0; i < iters; i++) {
                mesh_rdma_post_send(conn, buf, size, mr->mr->lkey, i);
                int done = 0;
                while (!done) mesh_rdma_poll(conn, &done, NULL);
            }
            double elapsed = now_sec() - start;
            double bw_gbs = (double)size * iters / elapsed / 1e9;

            printf("%-12zu %12d %12.1f %12.2f\n",
                   size, iters, elapsed * 1e3, bw_gbs);
        }
    }

    if (!is_server) {
        printf("\nDone. If you see > 10 GB/s on large messages, RDMA is flying.\n");
        printf("Compare to ~1.2 GB/s over 10GbE TCP.\n");
        printf("\nThe fat pipe is open.\n");
    }

    mesh_rdma_dereg_mr(mr);
    free(buf);
    mesh_rdma_close(conn);
    mesh_rdma_close_listen(listen_state);
    mesh_rdma_destroy(ctx);
    return 0;
}
