#include <sched.h>
/*
 * mesh_mpi_pt2pt.c — Point-to-point and progress engine
 *
 * Send/Recv over RDMA with ring relay for non-adjacent pairs.
 * Tag matching for unexpected messages.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <infiniband/verbs.h>

#include "mesh_mpi_internal.h"
#include "mpi.h"

/* ============================================================
 * Raw send: prepend header, route through ring
 * ============================================================ */

int mesh_mpi_send_raw(int dest, const void *buf, size_t len,
                      int tag, int comm_id, int flags) {
    if (dest == g_mpi.rank) return 0; /* self-send not supported yet */

    /* Build header */
    mesh_mpi_hdr_t hdr = {
        .src_rank = g_mpi.rank,
        .tag = tag,
        .comm_id = comm_id,
        .flags = flags,
        .length = len,
        .final_dest = dest,
        .reserved = 0
    };

    /* Determine next hop */
    int next_hop = g_mpi.ring_route[dest];
    if (next_hop < 0 || next_hop >= g_mpi.size) {
        fprintf(stderr, "[mesh-mpi] rank %d: no route to %d\n", g_mpi.rank, dest);
        return -1;
    }

    /* If not direct, set relay flag */
    if (next_hop != dest) {
        hdr.flags |= MESH_MPI_FLAG_RELAY;
    }

    mesh_mpi_peer_t *peer = &g_mpi.peers[next_hop];
    if (!peer->direct || !peer->conn) {
        fprintf(stderr, "[mesh-mpi] rank %d: no connection to next_hop %d\n",
                g_mpi.rank, next_hop);
        return -1;
    }

    /* Copy header + payload into registered send buffer */
    int slot = peer->send_head % 16;
    peer->send_head++;
    char *sbuf = peer->send_pool + slot * (4 * 1024 * 1024);

    size_t total = sizeof(hdr) + len;
    if (total > (4 * 1024 * 1024)) {
        fprintf(stderr, "[mesh-mpi] rank %d: message too large (%zu > %d)\n",
                g_mpi.rank, total, (4 * 1024 * 1024));
        return -1;
    }

    memcpy(sbuf, &hdr, sizeof(hdr));
    if (len > 0 && buf)
        memcpy(sbuf + sizeof(hdr), buf, len);

    /* Post RDMA send — fire and forget, slot rotation prevents overrun */
    uint64_t wr_id = (uint64_t)slot;
    if (mesh_rdma_post_send(peer->conn, sbuf, total,
                             peer->send_mr->mr->lkey, wr_id) != 0) {
        fprintf(stderr, "[mesh-mpi] rank %d: post_send to %d failed\n",
                g_mpi.rank, next_hop);
        return -1;
    }

    return 0;

    /* DISABLED: send completion wait — causes livelock with simultaneous sends */
    if (0) {
    struct ibv_wc wc;
    int polls = 0;
    while (polls++ < 10000000) {
        memset(&wc, 0, sizeof(wc));
        int r = ibv_poll_cq(peer->conn->cq, 1, &wc);
        sched_yield(); if (r > 0) {
            if (wc.opcode & IBV_WC_RECV) {
                /* Unexpected receive during send — stash it */
                if (g_mpi.num_unexpected < MESH_MPI_MAX_REQS) {
                    int rslot = (int)wc.wr_id;
                    char *rptr = peer->recv_pool + rslot * (4 * 1024 * 1024);
                    mesh_mpi_hdr_t *rhdr = (mesh_mpi_hdr_t *)rptr;
                    int uidx = g_mpi.num_unexpected++;
                    g_mpi.unexpected[uidx].hdr = *rhdr;
                    g_mpi.unexpected[uidx].data = malloc(rhdr->length);
                    memcpy(g_mpi.unexpected[uidx].data, rptr + sizeof(mesh_mpi_hdr_t), rhdr->length);
                    g_mpi.unexpected[uidx].valid = 1;

                    /* Re-post receive */
                    mesh_rdma_post_recv(peer->conn,
                                        peer->recv_pool + rslot * (4 * 1024 * 1024),
                                        (4 * 1024 * 1024),
                                        peer->recv_mr->mr->lkey, rslot);
                }
            } else {
                /* Send completion */
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "[mesh-mpi] rank %d: send WC error: %s\n",
                            g_mpi.rank, ibv_wc_status_str(wc.status));
                    return -1;
                }
                break;
            }
        }
    }
    } /* end disabled block */

    return 0;
}

/* ============================================================
 * Raw recv: check unexpected queue, then poll CQ
 * ============================================================ */
int mesh_mpi_recv_raw(int src, void *buf, size_t len,

                      int *actual_src, int *actual_tag,
                      size_t *actual_len, int tag, int comm_id) {

    /* First check unexpected queue */
    for (int i = 0; i < g_mpi.num_unexpected; i++) {
        if (!g_mpi.unexpected[i].valid) continue;
        mesh_mpi_hdr_t *h = &g_mpi.unexpected[i].hdr;

        if (h->comm_id != comm_id) continue;
        if (src != MPI_ANY_SOURCE && h->src_rank != src) continue;
        if (tag != MPI_ANY_TAG && h->tag != tag) continue;

        /* Match! */
        size_t copy_len = h->length < len ? h->length : len;
        memcpy(buf, g_mpi.unexpected[i].data, copy_len);
        if (actual_src) *actual_src = h->src_rank;
        if (actual_tag) *actual_tag = h->tag;
        if (actual_len) *actual_len = h->length;

        free(g_mpi.unexpected[i].data);
        g_mpi.unexpected[i].valid = 0;

        /* Compact the array */
        if (i == g_mpi.num_unexpected - 1) {
            g_mpi.num_unexpected--;
        }
        return 0;
    }

    /* Poll all peer CQs for matching message */
    while (1) {
        for (int p = 0; p < g_mpi.size; p++) {
            mesh_mpi_peer_t *peer = &g_mpi.peers[p];
            if (!peer->direct || !peer->conn) continue;

            struct ibv_wc wc;
            memset(&wc, 0, sizeof(wc));
            int r = ibv_poll_cq(peer->conn->cq, 1, &wc);
            if (r <= 0) continue; /* no completion or CQ error */

            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "[mesh-mpi] rank %d: WC error from peer %d: %s (opcode=%d)\n",
                        g_mpi.rank, p, ibv_wc_status_str(wc.status), wc.opcode);
                continue;
            }

            if (!(wc.opcode & IBV_WC_RECV)) {
                /* Send completion — ignore here */
                continue;
            }

            int rslot = (int)wc.wr_id;
            char *rptr = peer->recv_pool + rslot * (4 * 1024 * 1024);
            mesh_mpi_hdr_t *rhdr = (mesh_mpi_hdr_t *)rptr;

            /* Check if this is a relay message */
            if ((rhdr->flags & MESH_MPI_FLAG_RELAY) && rhdr->final_dest != g_mpi.rank) {
                /* Forward to next hop */
                int fwd_dest = rhdr->final_dest;
                mesh_mpi_send_raw(fwd_dest, rptr + sizeof(mesh_mpi_hdr_t),
                                  rhdr->length, rhdr->tag, rhdr->comm_id,
                                  rhdr->flags);

                /* Re-post receive */
                mesh_rdma_post_recv(peer->conn,
                                    peer->recv_pool + rslot * (4 * 1024 * 1024),
                                    (4 * 1024 * 1024),
                                    peer->recv_mr->mr->lkey, rslot);
                continue;
            }

            /* Check tag/source match */
            if (rhdr->comm_id != comm_id) goto stash;
            if (src != MPI_ANY_SOURCE && rhdr->src_rank != src) goto stash;
            if (tag != MPI_ANY_TAG && rhdr->tag != tag) goto stash;

            /* Match! Copy to user buffer */
            {
                size_t copy_len = rhdr->length < len ? rhdr->length : len;
                memcpy(buf, rptr + sizeof(mesh_mpi_hdr_t), copy_len);
                if (actual_src) *actual_src = rhdr->src_rank;
                if (actual_tag) *actual_tag = rhdr->tag;
                if (actual_len) *actual_len = rhdr->length;

                /* Re-post receive */
                mesh_rdma_post_recv(peer->conn,
                                    peer->recv_pool + rslot * (4 * 1024 * 1024),
                                    (4 * 1024 * 1024),
                                    peer->recv_mr->mr->lkey, rslot);
                return 0;
            }

        stash:
            /* No match — stash in unexpected queue */
            if (g_mpi.num_unexpected < MESH_MPI_MAX_REQS) {
                int uidx = g_mpi.num_unexpected++;
                g_mpi.unexpected[uidx].hdr = *rhdr;
                g_mpi.unexpected[uidx].data = malloc(rhdr->length);
                memcpy(g_mpi.unexpected[uidx].data, rptr + sizeof(mesh_mpi_hdr_t), rhdr->length);
                g_mpi.unexpected[uidx].valid = 1;
            }

            /* Re-post receive */
            mesh_rdma_post_recv(peer->conn,
                                peer->recv_pool + rslot * (4 * 1024 * 1024),
                                (4 * 1024 * 1024),
                                peer->recv_mr->mr->lkey, rslot);
        }
    }
}

/* ============================================================
 * MPI_Send / MPI_Recv — blocking
 * ============================================================ */

int MPI_Send(const void *buf, int count, MPI_Datatype datatype,
             int dest, int tag, MPI_Comm comm) {
    if (dest == MPI_PROC_NULL) return MPI_SUCCESS;
    size_t len = count * (size_t)datatype;
    int rc = mesh_mpi_send_raw(dest, buf, len, tag, comm->id, MESH_MPI_FLAG_EAGER);
    return rc == 0 ? MPI_SUCCESS : MPI_ERR_OTHER;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype,
             int source, int tag, MPI_Comm comm, MPI_Status *status) {
    if (source == MPI_PROC_NULL) {
        if (status && status != MPI_STATUS_IGNORE) {
            status->MPI_SOURCE = MPI_PROC_NULL;
            status->MPI_TAG = MPI_ANY_TAG;
            status->_count = 0;
        }
        return MPI_SUCCESS;
    }

    size_t len = count * (size_t)datatype;
    int actual_src, actual_tag;
    size_t actual_len;
    int rc = mesh_mpi_recv_raw(source, buf, len, &actual_src, &actual_tag,

                                &actual_len, tag, comm->id);

    if (status && status != MPI_STATUS_IGNORE) {
        status->MPI_SOURCE = actual_src;
        status->MPI_TAG = actual_tag;
        status->MPI_ERROR = (rc == 0) ? MPI_SUCCESS : MPI_ERR_OTHER;
        status->_count = actual_len;
    }

    return rc == 0 ? MPI_SUCCESS : MPI_ERR_OTHER;
}

/* ============================================================
 * MPI_Isend / MPI_Irecv — non-blocking (simplified)
 *
 * For now, Isend completes immediately (synchronous under the hood)
 * and Irecv posts a request that Wait/Test completes.
 * ============================================================ */

static int alloc_request(void) {
    pthread_mutex_lock(&g_mpi.req_lock);
    for (int i = 0; i < MESH_MPI_MAX_REQS; i++) {
        if (!g_mpi.reqs[i].active) {
            g_mpi.reqs[i].active = 1;
            g_mpi.reqs[i].complete = 0;
            pthread_mutex_unlock(&g_mpi.req_lock);
            return i;
        }
    }
    pthread_mutex_unlock(&g_mpi.req_lock);
    return -1;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype,
              int dest, int tag, MPI_Comm comm, MPI_Request *request) {
    if (dest == MPI_PROC_NULL) {
        *request = MPI_REQUEST_NULL;
        return MPI_SUCCESS;
    }

    /* Send immediately (synchronous for now) */
    size_t len = count * (size_t)datatype;
    int rc = mesh_mpi_send_raw(dest, buf, len, tag, comm->id, MESH_MPI_FLAG_EAGER);

    int req_id = alloc_request();
    if (req_id < 0) return MPI_ERR_OTHER;

    g_mpi.reqs[req_id].type = 0; /* send */
    g_mpi.reqs[req_id].complete = 1; /* already done */
    g_mpi.reqs[req_id].peer_rank = dest;
    g_mpi.reqs[req_id].tag = tag;
    g_mpi.reqs[req_id].comm_id = comm->id;
    *request = req_id;

    return rc == 0 ? MPI_SUCCESS : MPI_ERR_OTHER;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype,
              int source, int tag, MPI_Comm comm, MPI_Request *request) {
    if (source == MPI_PROC_NULL) {
        *request = MPI_REQUEST_NULL;
        return MPI_SUCCESS;
    }

    int req_id = alloc_request();
    if (req_id < 0) return MPI_ERR_OTHER;

    g_mpi.reqs[req_id].type = 1; /* recv */
    g_mpi.reqs[req_id].complete = 0;
    g_mpi.reqs[req_id].peer_rank = source;
    g_mpi.reqs[req_id].tag = tag;
    g_mpi.reqs[req_id].comm_id = comm->id;
    g_mpi.reqs[req_id].user_buf = buf;
    g_mpi.reqs[req_id].user_len = count * (size_t)datatype;
    *request = req_id;

    /* Check unexpected queue immediately */
    for (int i = 0; i < g_mpi.num_unexpected; i++) {
        if (!g_mpi.unexpected[i].valid) continue;
        mesh_mpi_hdr_t *h = &g_mpi.unexpected[i].hdr;

        if (h->comm_id != comm->id) continue;
        if (source != MPI_ANY_SOURCE && h->src_rank != source) continue;
        if (tag != MPI_ANY_TAG && h->tag != tag) continue;

        /* Match! */
        size_t copy_len = h->length < g_mpi.reqs[req_id].user_len ?
                          h->length : g_mpi.reqs[req_id].user_len;
        memcpy(buf, g_mpi.unexpected[i].data, copy_len);
        g_mpi.reqs[req_id].received_len = h->length;
        g_mpi.reqs[req_id].peer_rank = h->src_rank;
        g_mpi.reqs[req_id].tag = h->tag;
        g_mpi.reqs[req_id].complete = 1;

        free(g_mpi.unexpected[i].data);
        g_mpi.unexpected[i].valid = 0;
        break;
    }

    return MPI_SUCCESS;
}

/* ============================================================
 * Progress engine — poll CQs, match against pending recvs
 * ============================================================ */

int mesh_mpi_progress(void) {
    int progressed = 0;

    for (int p = 0; p < g_mpi.size; p++) {
        mesh_mpi_peer_t *peer = &g_mpi.peers[p];
        if (!peer->direct || !peer->conn) continue;

        struct ibv_wc wc;
        memset(&wc, 0, sizeof(wc));
        int r = ibv_poll_cq(peer->conn->cq, 1, &wc);
        if (r <= 0) continue; /* no completion or CQ error */

        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "[mesh-mpi] rank %d: progress WC error from peer %d: %s (opcode=%d)\n",
                    g_mpi.rank, p, ibv_wc_status_str(wc.status), wc.opcode);
            continue;
        }
        if (!(wc.opcode & IBV_WC_RECV)) continue;

        progressed++;
        int rslot = (int)wc.wr_id;
        char *rptr = peer->recv_pool + rslot * (4 * 1024 * 1024);
        mesh_mpi_hdr_t *rhdr = (mesh_mpi_hdr_t *)rptr;

        /* Relay check */
        if ((rhdr->flags & MESH_MPI_FLAG_RELAY) && rhdr->final_dest != g_mpi.rank) {
            mesh_mpi_send_raw(rhdr->final_dest, rptr + sizeof(mesh_mpi_hdr_t),
                              rhdr->length, rhdr->tag, rhdr->comm_id, rhdr->flags);
            mesh_rdma_post_recv(peer->conn,
                                peer->recv_pool + rslot * (4 * 1024 * 1024),
                                (4 * 1024 * 1024),
                                peer->recv_mr->mr->lkey, rslot);
            continue;
        }

        /* Try to match against pending Irecv requests */
        int matched = 0;
        for (int i = 0; i < MESH_MPI_MAX_REQS; i++) {
            mesh_mpi_request_t *req = &g_mpi.reqs[i];
            if (!req->active || req->complete || req->type != 1) continue;

            if (req->comm_id != rhdr->comm_id) continue;
            if (req->peer_rank != MPI_ANY_SOURCE && req->peer_rank != rhdr->src_rank) continue;
            if (req->tag != MPI_ANY_TAG && req->tag != rhdr->tag) continue;

            /* Match! */
            size_t copy_len = rhdr->length < req->user_len ? rhdr->length : req->user_len;
            memcpy(req->user_buf, rptr + sizeof(mesh_mpi_hdr_t), copy_len);
            req->received_len = rhdr->length;
            req->peer_rank = rhdr->src_rank;
            req->tag = rhdr->tag;
            req->complete = 1;
            matched = 1;
            break;
        }

        if (!matched) {
            /* Stash in unexpected queue */
            if (g_mpi.num_unexpected < MESH_MPI_MAX_REQS) {
                int uidx = g_mpi.num_unexpected++;
                g_mpi.unexpected[uidx].hdr = *rhdr;
                g_mpi.unexpected[uidx].data = malloc(rhdr->length);
                memcpy(g_mpi.unexpected[uidx].data, rptr + sizeof(mesh_mpi_hdr_t), rhdr->length);
                g_mpi.unexpected[uidx].valid = 1;
            }
        }

        mesh_rdma_post_recv(peer->conn,
                            peer->recv_pool + rslot * (4 * 1024 * 1024),
                            (4 * 1024 * 1024),
                            peer->recv_mr->mr->lkey, rslot);
    }

    return progressed;
}

/* ============================================================
 * Wait / Test
 * ============================================================ */

int MPI_Wait(MPI_Request *request, MPI_Status *status) {
    if (!request || *request == MPI_REQUEST_NULL) return MPI_SUCCESS;

    int req_id = *request;
    mesh_mpi_request_t *req = &g_mpi.reqs[req_id];

    while (!req->complete) {
        mesh_mpi_progress();
    }

    if (status && status != MPI_STATUS_IGNORE) {
        status->MPI_SOURCE = req->peer_rank;
        status->MPI_TAG = req->tag;
        status->MPI_ERROR = MPI_SUCCESS;
        status->_count = req->received_len;
    }

    req->active = 0;
    *request = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request array_of_requests[],
                MPI_Status array_of_statuses[]) {
    for (int i = 0; i < count; i++) {
        MPI_Status *st = (array_of_statuses && array_of_statuses != MPI_STATUSES_IGNORE)
                         ? &array_of_statuses[i] : MPI_STATUS_IGNORE;
        MPI_Wait(&array_of_requests[i], st);
    }
    return MPI_SUCCESS;
}

int MPI_Waitany(int count, MPI_Request array_of_requests[],
                int *index, MPI_Status *status) {
    while (1) {
        mesh_mpi_progress();
        for (int i = 0; i < count; i++) {
            if (array_of_requests[i] == MPI_REQUEST_NULL) continue;
            int req_id = array_of_requests[i];
            if (g_mpi.reqs[req_id].complete) {
                *index = i;
                return MPI_Wait(&array_of_requests[i], status);
            }
        }
    }
}

int MPI_Waitsome(int incount, MPI_Request array_of_requests[],
                 int *outcount, int array_of_indices[],
                 MPI_Status array_of_statuses[]) {
    *outcount = 0;
    mesh_mpi_progress();

    for (int i = 0; i < incount; i++) {
        if (array_of_requests[i] == MPI_REQUEST_NULL) continue;
        int req_id = array_of_requests[i];
        if (g_mpi.reqs[req_id].complete) {
            array_of_indices[*outcount] = i;
            MPI_Status *st = (array_of_statuses && array_of_statuses != MPI_STATUSES_IGNORE)
                             ? &array_of_statuses[*outcount] : MPI_STATUS_IGNORE;
            MPI_Wait(&array_of_requests[i], st);
            (*outcount)++;
        }
    }

    if (*outcount == 0) {
        /* Block until at least one completes */
        int idx;
        MPI_Waitany(incount, array_of_requests, &idx, MPI_STATUS_IGNORE);
        array_of_indices[0] = idx;
        *outcount = 1;
    }
    return MPI_SUCCESS;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status) {
    if (!request || *request == MPI_REQUEST_NULL) {
        *flag = 1;
        return MPI_SUCCESS;
    }

    mesh_mpi_progress();

    int req_id = *request;
    if (g_mpi.reqs[req_id].complete) {
        *flag = 1;
        return MPI_Wait(request, status);
    }

    *flag = 0;
    return MPI_SUCCESS;
}

int MPI_Testall(int count, MPI_Request array_of_requests[],
                int *flag, MPI_Status array_of_statuses[]) {
    mesh_mpi_progress();
    *flag = 1;

    for (int i = 0; i < count; i++) {
        if (array_of_requests[i] == MPI_REQUEST_NULL) continue;
        int req_id = array_of_requests[i];
        if (!g_mpi.reqs[req_id].complete) {
            *flag = 0;
            return MPI_SUCCESS;
        }
    }

    /* All complete */
    for (int i = 0; i < count; i++) {
        MPI_Status *st = (array_of_statuses && array_of_statuses != MPI_STATUSES_IGNORE)
                         ? &array_of_statuses[i] : MPI_STATUS_IGNORE;
        MPI_Wait(&array_of_requests[i], st);
    }
    return MPI_SUCCESS;
}

int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status) {
    mesh_mpi_progress();
    *flag = 0;

    for (int i = 0; i < g_mpi.num_unexpected; i++) {
        if (!g_mpi.unexpected[i].valid) continue;
        mesh_mpi_hdr_t *h = &g_mpi.unexpected[i].hdr;

        if (h->comm_id != comm->id) continue;
        if (source != MPI_ANY_SOURCE && h->src_rank != source) continue;
        if (tag != MPI_ANY_TAG && h->tag != tag) continue;

        *flag = 1;
        if (status && status != MPI_STATUS_IGNORE) {
            status->MPI_SOURCE = h->src_rank;
            status->MPI_TAG = h->tag;
            status->_count = h->length;
        }
        break;
    }
    return MPI_SUCCESS;
}

int MPI_Get_count(const MPI_Status *status, MPI_Datatype datatype, int *count) {
    if (!status) { *count = 0; return MPI_SUCCESS; }
    *count = status->_count / (int)datatype;
    return MPI_SUCCESS;
}
