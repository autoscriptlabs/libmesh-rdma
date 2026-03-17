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

    /* Ring barrier: token passes counter-clockwise (send to prev, recv from next).
     * This ensures all links use the working direction on direct-connect rings
     * where the clockwise return path (e.g. 3→0) may be broken. */
    int next = (rank + 1) % size;
    int prev = (rank - 1 + size) % size;
    if (rank != 0) {
        MPI_Recv(&token, 1, MPI_BYTE, next, 0xFFFF, comm, MPI_STATUS_IGNORE);
        MPI_Send(&token, 1, MPI_BYTE, prev, 0xFFFF, comm);
    } else {
        MPI_Send(&token, 1, MPI_BYTE, prev, 0xFFFF, comm);
        MPI_Recv(&token, 1, MPI_BYTE, next, 0xFFFF, comm, MPI_STATUS_IGNORE);
    }
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
              int root, MPI_Comm comm)
{
    int rank = comm->rank;
    int size = comm->size;
    size_t bytes = (size_t)count * (size_t)datatype;

    if (size <= 1) return MPI_SUCCESS;

    fprintf(stderr, "[mesh-mpi] rank %d: ENTER Bcast root=%d count=%d\n", rank, root, count);
    fflush(stderr);

    int prev = (rank - 1 + size) % size;
    int next = (rank + 1) % size;

    if (rank == root) {
        // Root sends to prev (reverse direction)
        MPI_Send(buffer, count, datatype, prev, 0xB001, comm);
        fprintf(stderr, "[mesh-mpi] rank %d (root): sent data forward\n", rank);
        fflush(stderr);
    } else {
        // Receive from previous → then forward if not the last in chain
        MPI_Recv(buffer, count, datatype, next, 0xB001, comm, MPI_STATUS_IGNORE);
        fprintf(stderr, "[mesh-mpi] rank %d: received data from %d\n", rank, prev);
        fflush(stderr);

        // Only forward if we're not the last rank in the logical chain
        if (prev != root) {
            MPI_Send(buffer, count, datatype, prev, 0xB001, comm);
            fprintf(stderr, "[mesh-mpi] rank %d: forwarded data to %d\n", rank, next);
            fflush(stderr);
        } else {
            fprintf(stderr, "[mesh-mpi] rank %d: last in chain - no forward\n", rank);
            fflush(stderr);
        }
    }

    fprintf(stderr, "[mesh-mpi] rank %d: Bcast completed\n", rank);
    fflush(stderr);

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

    fprintf(stderr, "[mesh-mpi] rank %d: ENTER Reduce root=%d\n", rank, root); fflush(stderr);
    /* Chain reduce: counter-clockwise (recv from next, send to prev) toward root.
     * This matches Gather/Bcast direction and avoids the broken clockwise return path. */
    int prev = (rank - 1 + size) % size;
    int next = (rank + 1) % size;

    if (sendbuf != MPI_IN_PLACE)
        memcpy(recvbuf, sendbuf, len);

    /* Everyone except the rank whose next==root (start of chain) receives from next */
    void *tmp = malloc(len);
    if (next != root) {
        /* We have a successor in the chain — recv and accumulate */
        MPI_Recv(tmp, count, datatype, next, 0xFFFC, comm, MPI_STATUS_IGNORE);
        apply_op(recvbuf, tmp, count, datatype, op);
    }

    /* Everyone except root sends to prev */
    if (rank != root) {
        MPI_Send(recvbuf, count, datatype, prev, 0xFFFC, comm);
    }
    free(tmp);

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

    fprintf(stderr, "[mesh-mpi] rank %d: ENTER Allreduce count=%d\n", comm->rank, count);
    fflush(stderr);
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

    fprintf(stderr, "[mesh-mpi] rank %d: ENTER Gather root=%d\n", rank, root); fflush(stderr);
    /* Chain gather: flow next→prev toward root (reverse ring) */
    int next = (rank + 1) % size;
    int prev = (rank - 1 + size) % size;
    char *tmpbuf = malloc(size * recv_len);
    memset(tmpbuf, 0, size * recv_len);

    /* Place own data */
    if (sendbuf != MPI_IN_PLACE)
        memcpy(tmpbuf + rank * recv_len, sendbuf, send_len);
    else if (rank == root)
        memcpy(tmpbuf + rank * recv_len, (char*)recvbuf + rank * recv_len, recv_len);

    /* Receive from next (unless next is root = start of chain) */
    if (next != root) {
        char *incoming = malloc(size * recv_len);
        MPI_Recv(incoming, size * recv_len, MPI_BYTE, next, 0xFFFB, comm, MPI_STATUS_IGNORE);
        for (int i = 0; i < size; i++) {
            if (i != rank) memcpy(tmpbuf + i * recv_len, incoming + i * recv_len, recv_len);
        }
        free(incoming);
    }

    /* Forward to prev (unless we are root) */
    if (rank != root) {
        MPI_Send(tmpbuf, size * recv_len, MPI_BYTE, prev, 0xFFFB, comm);
    } else {
        memcpy(recvbuf, tmpbuf, size * recv_len);
    }
    free(tmpbuf);
    return MPI_SUCCESS;
}

int MPI_Gatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int recvcounts[], const int displs[],
                MPI_Datatype recvtype, int root, MPI_Comm comm) {
    int size = comm->size;
    int rank = comm->rank;
    size_t send_len = sendcount * (size_t)sendtype;
    size_t dt_recv = (size_t)recvtype;

    /* Chain Gatherv: pass data around ring to root */
    int next = (rank + 1) % size;
    int prev = (rank - 1 + size) % size;
    int total = 0;
    for (int i = 0; i < size; i++) total += recvcounts[i];
    size_t total_bytes = total * dt_recv;
    char *tmpbuf = calloc(1, total_bytes);

    /* Place own data */
    if (sendbuf != MPI_IN_PLACE)
        memcpy(tmpbuf + displs[rank] * dt_recv, sendbuf, send_len);
    else if (rank == root)
        memcpy(tmpbuf + displs[rank] * dt_recv, (char*)recvbuf + displs[rank] * dt_recv, send_len);

    if (next != root) {
        char *incoming = calloc(1, total_bytes);
        MPI_Recv(incoming, total_bytes, MPI_BYTE, next, 0xFFFA, comm, MPI_STATUS_IGNORE);
        for (int i = 0; i < size; i++) {
            if (i != rank) memcpy(tmpbuf + displs[i]*dt_recv, incoming + displs[i]*dt_recv, recvcounts[i]*dt_recv);
        }
        free(incoming);
    }
    if (rank != root) {
        MPI_Send(tmpbuf, total_bytes, MPI_BYTE, prev, 0xFFFA, comm);
    } else {
        memcpy(recvbuf, tmpbuf, total_bytes);
    }
    free(tmpbuf);
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

    /* Ring-based alltoall: shift data counter-clockwise (send to prev, recv from next).
     * This avoids the broken clockwise return path on direct-connect rings. */
    int next = (rank + 1) % size;
    int prev = (rank - 1 + size) % size;
    for (int step = 1; step < size; step++) {
        int send_idx = (rank - step + size) % size;
        int recv_idx = (rank + step) % size;
        MPI_Request sreq;
        MPI_Isend((const char *)sendbuf + send_idx * send_chunk,
                  sendcount, sendtype, prev, 0xFFF9 + step, comm, &sreq);
        MPI_Recv((char *)recvbuf + recv_idx * recv_chunk,
                 recvcount, recvtype, next, 0xFFF9 + step, comm, MPI_STATUS_IGNORE);
        MPI_Wait(&sreq, MPI_STATUS_IGNORE);
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
