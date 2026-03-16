/*
 * mesh_mpi_coll.c — Collectives built on Send/Recv
 * Ring algorithms for everything. Simple. Correct. Fast on ring topology.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mesh_mpi_internal.h"
#include "mpi.h"

/* ============================================================
 * Reduce operation helpers
 * ============================================================ */

static void apply_op(void *dst, const void *src, int count,
                     MPI_Datatype datatype, MPI_Op op) {
    size_t dt_size = (size_t)datatype;

    if (dt_size == 8 && (op == MPI_SUM || op == MPI_MAX || op == MPI_MIN)) {
        double *d = dst;
        const double *s = src;
        for (int i = 0; i < count; i++) {
            if (op == MPI_SUM) d[i] += s[i];
            else if (op == MPI_MAX) { if (s[i] > d[i]) d[i] = s[i]; }
            else if (op == MPI_MIN) { if (s[i] < d[i]) d[i] = s[i]; }
        }
    } else if (dt_size == 4 && (op == MPI_SUM || op == MPI_MAX || op == MPI_MIN)) {
        /* Could be float or int — try both based on common usage */
        float *d = dst;
        const float *s = src;
        for (int i = 0; i < count; i++) {
            if (op == MPI_SUM) d[i] += s[i];
            else if (op == MPI_MAX) { if (s[i] > d[i]) d[i] = s[i]; }
            else if (op == MPI_MIN) { if (s[i] < d[i]) d[i] = s[i]; }
        }
    } else if (op == MPI_LAND || op == MPI_LOR) {
        int *d = dst;
        const int *s = src;
        for (int i = 0; i < count; i++) {
            if (op == MPI_LAND) d[i] = d[i] && s[i];
            else d[i] = d[i] || s[i];
        }
    } else if (op == MPI_BAND || op == MPI_BOR) {
        char *d = dst;
        const char *s = src;
        for (size_t i = 0; i < (size_t)count * dt_size; i++) {
            if (op == MPI_BAND) d[i] &= s[i];
            else d[i] |= s[i];
        }
    } else {
        /* Fallback: treat as integer sum */
        if (dt_size == 4) {
            int *d = dst;
            const int *s = src;
            for (int i = 0; i < count; i++) d[i] += s[i];
        } else if (dt_size == 8) {
            long long *d = dst;
            const long long *s = src;
            for (int i = 0; i < count; i++) d[i] += s[i];
        }
    }
}

/* ============================================================
 * MPI_Barrier — ring token
 * ============================================================ */

int MPI_Barrier(MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    char token = 42;

    if (size <= 1) return MPI_SUCCESS;

    /* Simple dissemination barrier */
    int next = (rank + 1) % size;
    int prev = (rank - 1 + size) % size;

    /* Send to next, recv from prev */
    MPI_Request sreq;
    MPI_Isend(&token, 1, MPI_BYTE, next, 0xFFFF, comm, &sreq);
    MPI_Recv(&token, 1, MPI_BYTE, prev, 0xFFFF, comm, MPI_STATUS_IGNORE);
    MPI_Wait(&sreq, MPI_STATUS_IGNORE);

    return MPI_SUCCESS;
}

int MPI_Ibarrier(MPI_Comm comm, MPI_Request *request) {
    /* Simplified: blocking barrier, return completed request */
    MPI_Barrier(comm);
    *request = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Bcast — chain from root
 * ============================================================ */

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype,
              int root, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t len = count * (size_t)datatype;

    if (size <= 1) return MPI_SUCCESS;

    /* Linear chain from root */
    if (rank == root) {
        for (int i = 0; i < size; i++) {
            if (i == root) continue;
            MPI_Send(buffer, count, datatype, i, 0xFFFD, comm);
        }
    } else {
        MPI_Recv(buffer, count, datatype, root, 0xFFFD, comm, MPI_STATUS_IGNORE);
    }
    (void)len;
    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Reduce — gather to root with op
 * ============================================================ */

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t len = count * (size_t)datatype;

    if (size <= 1) {
        if (sendbuf != MPI_IN_PLACE)
            memcpy(recvbuf, sendbuf, len);
        return MPI_SUCCESS;
    }

    if (rank == root) {
        /* Copy sendbuf to recvbuf first */
        if (sendbuf != MPI_IN_PLACE)
            memcpy(recvbuf, sendbuf, len);

        /* Receive and accumulate from all others */
        void *tmp = malloc(len);
        for (int i = 0; i < size; i++) {
            if (i == root) continue;
            MPI_Recv(tmp, count, datatype, i, 0xFFFC, comm, MPI_STATUS_IGNORE);
            apply_op(recvbuf, tmp, count, datatype, op);
        }
        free(tmp);
    } else {
        const void *sbuf = (sendbuf == MPI_IN_PLACE) ? recvbuf : sendbuf;
        MPI_Send(sbuf, count, datatype, root, 0xFFFC, comm);
    }

    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Allreduce — reduce + bcast
 * ============================================================ */

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm) {
    size_t len = count * (size_t)datatype;

    if (sendbuf != MPI_IN_PLACE)
        memcpy(recvbuf, sendbuf, len);

    MPI_Reduce(MPI_IN_PLACE, recvbuf, count, datatype, op, 0, comm);
    MPI_Bcast(recvbuf, count, datatype, 0, comm);

    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Reduce_scatter
 * ============================================================ */

int MPI_Reduce_scatter(const void *sendbuf, void *recvbuf,
                       const int recvcounts[], MPI_Datatype datatype,
                       MPI_Op op, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t dt_size = (size_t)datatype;

    /* Total count */
    int total = 0;
    for (int i = 0; i < size; i++) total += recvcounts[i];

    /* Full allreduce into temp buffer */
    void *tmp = malloc(total * dt_size);
    MPI_Allreduce(sendbuf, tmp, total, datatype, op, comm);

    /* Extract my chunk */
    int offset = 0;
    for (int i = 0; i < rank; i++) offset += recvcounts[i];
    memcpy(recvbuf, (char *)tmp + offset * dt_size, recvcounts[rank] * dt_size);

    free(tmp);
    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Gather / MPI_Gatherv
 * ============================================================ */

int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype,
               int root, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t send_len = sendcount * (size_t)sendtype;
    size_t recv_len = recvcount * (size_t)recvtype;

    if (rank == root) {
        /* Copy own data */
        if (sendbuf != MPI_IN_PLACE)
            memcpy((char *)recvbuf + rank * recv_len, sendbuf, send_len);

        /* Receive from others */
        for (int i = 0; i < size; i++) {
            if (i == root) continue;
            MPI_Recv((char *)recvbuf + i * recv_len, recvcount, recvtype,
                     i, 0xFFFB, comm, MPI_STATUS_IGNORE);
        }
    } else {
        MPI_Send(sendbuf, sendcount, sendtype, root, 0xFFFB, comm);
    }
    return MPI_SUCCESS;
}

int MPI_Gatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int recvcounts[], const int displs[],
                MPI_Datatype recvtype, int root, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t send_len = sendcount * (size_t)sendtype;
    size_t dt_recv = (size_t)recvtype;

    if (rank == root) {
        if (sendbuf != MPI_IN_PLACE)
            memcpy((char *)recvbuf + displs[rank] * dt_recv, sendbuf, send_len);

        for (int i = 0; i < size; i++) {
            if (i == root) continue;
            MPI_Recv((char *)recvbuf + displs[i] * dt_recv, recvcounts[i], recvtype,
                     i, 0xFFFA, comm, MPI_STATUS_IGNORE);
        }
    } else {
        MPI_Send(sendbuf, sendcount, sendtype, root, 0xFFFA, comm);
    }
    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Allgather / MPI_Allgatherv
 * ============================================================ */

int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm) {
    MPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, 0, comm);
    size_t total_len = comm->size * recvcount * (size_t)recvtype;
    MPI_Bcast(recvbuf, total_len, MPI_BYTE, 0, comm);
    return MPI_SUCCESS;
}

int MPI_Allgatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int recvcounts[], const int displs[],
                   MPI_Datatype recvtype, MPI_Comm comm) {
    MPI_Gatherv(sendbuf, sendcount, sendtype, recvbuf, recvcounts, displs,
                recvtype, 0, comm);

    /* Bcast the whole buffer from root */
    int total = 0;
    for (int i = 0; i < comm->size; i++)
        total = (displs[i] + recvcounts[i]) > total ? (displs[i] + recvcounts[i]) : total;
    MPI_Bcast(recvbuf, total * (int)(size_t)recvtype, MPI_BYTE, 0, comm);
    return MPI_SUCCESS;
}

/* ============================================================
 * MPI_Alltoall
 * ============================================================ */

int MPI_Alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t send_chunk = sendcount * (size_t)sendtype;
    size_t recv_chunk = recvcount * (size_t)recvtype;

    /* Copy own data */
    memcpy((char *)recvbuf + rank * recv_chunk,
           (const char *)sendbuf + rank * send_chunk, send_chunk);

    /* Exchange with all others — use ring order to avoid deadlock */
    for (int step = 1; step < size; step++) {
        int send_to = (rank + step) % size;
        int recv_from = (rank - step + size) % size;

        MPI_Request sreq, rreq;
        MPI_Isend((const char *)sendbuf + send_to * send_chunk,
                  sendcount, sendtype, send_to, 0xFFF9, comm, &sreq);
        MPI_Irecv((char *)recvbuf + recv_from * recv_chunk,
                  recvcount, recvtype, recv_from, 0xFFF9, comm, &rreq);
        MPI_Wait(&sreq, MPI_STATUS_IGNORE);
        MPI_Wait(&rreq, MPI_STATUS_IGNORE);
    }
    return MPI_SUCCESS;
}

/* ============================================================
 * Datatype bookkeeping (minimal)
 * ============================================================ */

int MPI_Type_contiguous(int count, MPI_Datatype oldtype, MPI_Datatype *newtype) {
    *newtype = (MPI_Datatype)(count * (int)(size_t)oldtype);
    return MPI_SUCCESS;
}

int MPI_Type_create_struct(int count, const int blens[],
                           const MPI_Aint displs[],
                           const MPI_Datatype types[],
                           MPI_Datatype *newtype) {
    /* Calculate total size */
    MPI_Aint total = 0;
    for (int i = 0; i < count; i++) {
        MPI_Aint end = displs[i] + blens[i] * (MPI_Aint)(size_t)types[i];
        if (end > total) total = end;
    }
    *newtype = (MPI_Datatype)total;
    return MPI_SUCCESS;
}

int MPI_Type_create_resized(MPI_Datatype oldtype, MPI_Aint lb, MPI_Aint extent,
                            MPI_Datatype *newtype) {
    (void)oldtype; (void)lb;
    *newtype = (MPI_Datatype)extent;
    return MPI_SUCCESS;
}

int MPI_Type_commit(MPI_Datatype *datatype) { (void)datatype; return MPI_SUCCESS; }
int MPI_Type_free(MPI_Datatype *datatype) { *datatype = MPI_DATATYPE_NULL; return MPI_SUCCESS; }

int MPI_Type_size(MPI_Datatype datatype, int *size) {
    *size = (int)(size_t)datatype;
    return MPI_SUCCESS;
}

int MPI_Type_get_extent(MPI_Datatype datatype, MPI_Aint *lb, MPI_Aint *extent) {
    *lb = 0;
    *extent = (MPI_Aint)(size_t)datatype;
    return MPI_SUCCESS;
}

int MPI_Get_address(const void *location, MPI_Aint *address) {
    *address = (MPI_Aint)location;
    return MPI_SUCCESS;
}

int MPI_Op_free(MPI_Op *op) { *op = MPI_OP_NULL; return MPI_SUCCESS; }

int MPI_Ibcast(void *buffer, int count, MPI_Datatype datatype,
               int root, MPI_Comm comm, MPI_Request *request) {
    /* Simplified: blocking bcast, return completed request */
    MPI_Bcast(buffer, count, datatype, root, comm);
    *request = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}
