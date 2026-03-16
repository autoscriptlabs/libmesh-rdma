/*
 * mpi.h — Minimal MPI standard header for libmesh-mpi
 *
 * Implements the subset of MPI-3.1 that AMReX/WarpX requires.
 * Drop-in replacement for mpi.h when linked against libmesh_mpi.so.
 */

#ifndef MESH_MPI_H
#define MESH_MPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================
 * Return codes
 * ============================================================ */

#define MPI_SUCCESS          0
#define MPI_ERR_COMM         1
#define MPI_ERR_TYPE         2
#define MPI_ERR_BUFFER       3
#define MPI_ERR_COUNT        4
#define MPI_ERR_TAG          5
#define MPI_ERR_RANK         6
#define MPI_ERR_GROUP        7
#define MPI_ERR_OP           8
#define MPI_ERR_REQUEST      9
#define MPI_ERR_INTERN       10
#define MPI_ERR_OTHER        15
#define MPI_ERR_IN_STATUS    16

/* ============================================================
 * Constants
 * ============================================================ */

#define MPI_MAX_PROCESSOR_NAME  256
#define MPI_MAX_ERROR_STRING    256
#define MPI_ANY_SOURCE          (-1)
#define MPI_ANY_TAG             (-1)
#define MPI_UNDEFINED           (-32766)
#define MPI_PROC_NULL           (-2)

/* Thread levels */
#define MPI_THREAD_SINGLE       0
#define MPI_THREAD_FUNNELED     1
#define MPI_THREAD_SERIALIZED   2
#define MPI_THREAD_MULTIPLE     3

/* Attribute keys */
#define MPI_TAG_UB              1
#define MPI_WTIME_IS_GLOBAL     3
#define MPI_APPNUM              4
#define MPI_UNIVERSE_SIZE       5
#define MPI_LASTUSEDCODE        6

/* Split type */
#define MPI_COMM_TYPE_SHARED    1

/* In-place flag */
#define MPI_IN_PLACE            ((void *)1)

/* ============================================================
 * Opaque types (forward declarations)
 * ============================================================ */

/* Communicator */
typedef struct mesh_mpi_comm_t *MPI_Comm;
extern MPI_Comm MPI_COMM_WORLD;
extern MPI_Comm MPI_COMM_SELF;
extern MPI_Comm MPI_COMM_NULL;

/* Datatype — encoded as size in bytes */
typedef int MPI_Datatype;

#define MPI_CHAR                ((MPI_Datatype)1)
#define MPI_BYTE                ((MPI_Datatype)1)
#define MPI_SHORT               ((MPI_Datatype)2)
#define MPI_INT                 ((MPI_Datatype)4)
#define MPI_LONG                ((MPI_Datatype)8)
#define MPI_LONG_LONG           ((MPI_Datatype)8)
#define MPI_LONG_LONG_INT       ((MPI_Datatype)8)
#define MPI_UNSIGNED            ((MPI_Datatype)4)
#define MPI_UNSIGNED_LONG       ((MPI_Datatype)8)
#define MPI_UNSIGNED_LONG_LONG  ((MPI_Datatype)8)
#define MPI_FLOAT               ((MPI_Datatype)4)
#define MPI_DOUBLE              ((MPI_Datatype)8)
#define MPI_LONG_DOUBLE         ((MPI_Datatype)16)
#define MPI_C_BOOL              ((MPI_Datatype)1)
#define MPI_INT8_T              ((MPI_Datatype)1)
#define MPI_INT16_T             ((MPI_Datatype)2)
#define MPI_INT32_T             ((MPI_Datatype)4)
#define MPI_INT64_T             ((MPI_Datatype)8)
#define MPI_UINT8_T             ((MPI_Datatype)1)
#define MPI_UINT16_T            ((MPI_Datatype)2)
#define MPI_UINT32_T            ((MPI_Datatype)4)
#define MPI_UINT64_T            ((MPI_Datatype)8)
#define MPI_AINT                ((MPI_Datatype)8)
#define MPI_PACKED              ((MPI_Datatype)1)
#define MPI_SIGNED_CHAR         ((MPI_Datatype)1)
#define MPI_UNSIGNED_CHAR       ((MPI_Datatype)1)
#define MPI_UNSIGNED_SHORT      ((MPI_Datatype)2)
#define MPI_WCHAR               ((MPI_Datatype)4)
#define MPI_C_FLOAT_COMPLEX     ((MPI_Datatype)8)
#define MPI_C_DOUBLE_COMPLEX    ((MPI_Datatype)16)
#define MPI_C_LONG_DOUBLE_COMPLEX ((MPI_Datatype)32)
#define MPI_DOUBLE_INT          ((MPI_Datatype)12)
#define MPI_FLOAT_INT           ((MPI_Datatype)8)
#define MPI_2INT                ((MPI_Datatype)8)
#define MPI_LONG_INT            ((MPI_Datatype)12)
#define MPI_SHORT_INT           ((MPI_Datatype)6)
#define MPI_2FLOAT              ((MPI_Datatype)8)
#define MPI_2DOUBLE             ((MPI_Datatype)16)
#define MPI_DATATYPE_NULL       ((MPI_Datatype)0)

/* Operation */
typedef int MPI_Op;

#define MPI_SUM                 1
#define MPI_MAX                 2
#define MPI_MIN                 3
#define MPI_PROD                4
#define MPI_LAND                5
#define MPI_LOR                 6
#define MPI_BAND                7
#define MPI_BOR                 8
#define MPI_MAXLOC              9
#define MPI_MINLOC              10
#define MPI_OP_NULL             0

/* Request */
typedef int MPI_Request;
#define MPI_REQUEST_NULL        (-1)

/* Status */
typedef struct {
    int MPI_SOURCE;
    int MPI_TAG;
    int MPI_ERROR;
    int _count;
} MPI_Status;
#define MPI_STATUS_IGNORE       ((MPI_Status *)NULL)
#define MPI_STATUSES_IGNORE     ((MPI_Status *)NULL)

/* Group */
typedef void *MPI_Group;
#define MPI_GROUP_EMPTY         ((MPI_Group)NULL)
#define MPI_GROUP_NULL          ((MPI_Group)NULL)

/* Info */
typedef void *MPI_Info;
#define MPI_INFO_NULL           ((MPI_Info)NULL)

/* Address */
typedef long MPI_Aint;
typedef int MPI_Count;

/* Errors fatal flag */
#define MPI_ERRORS_ARE_FATAL    1
#define MPI_ERRORS_RETURN       2

/* ============================================================
 * Init / Finalize
 * ============================================================ */

int MPI_Init(int *argc, char ***argv);
int MPI_Init_thread(int *argc, char ***argv, int required, int *provided);
int MPI_Finalize(void);
int MPI_Initialized(int *flag);
int MPI_Finalized(int *flag);
int MPI_Abort(MPI_Comm comm, int errorcode);

/* ============================================================
 * Communicator
 * ============================================================ */

int MPI_Comm_rank(MPI_Comm comm, int *rank);
int MPI_Comm_size(MPI_Comm comm, int *size);
int MPI_Comm_dup(MPI_Comm comm, MPI_Comm *newcomm);
int MPI_Comm_free(MPI_Comm *comm);
int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm *newcomm);
int MPI_Comm_split_type(MPI_Comm comm, int split_type, int key,
                        MPI_Info info, MPI_Comm *newcomm);
int MPI_Comm_create(MPI_Comm comm, MPI_Group group, MPI_Comm *newcomm);
int MPI_Comm_group(MPI_Comm comm, MPI_Group *group);
int MPI_Comm_get_attr(MPI_Comm comm, int keyval, void *attr, int *flag);

/* ============================================================
 * Group
 * ============================================================ */

int MPI_Group_incl(MPI_Group group, int n, const int ranks[], MPI_Group *newgroup);
int MPI_Group_translate_ranks(MPI_Group group1, int n, const int ranks1[],
                              MPI_Group group2, int ranks2[]);
int MPI_Group_free(MPI_Group *group);

/* ============================================================
 * Point-to-point
 * ============================================================ */

int MPI_Send(const void *buf, int count, MPI_Datatype datatype,
             int dest, int tag, MPI_Comm comm);
int MPI_Recv(void *buf, int count, MPI_Datatype datatype,
             int source, int tag, MPI_Comm comm, MPI_Status *status);
int MPI_Isend(const void *buf, int count, MPI_Datatype datatype,
              int dest, int tag, MPI_Comm comm, MPI_Request *request);
int MPI_Irecv(void *buf, int count, MPI_Datatype datatype,
              int source, int tag, MPI_Comm comm, MPI_Request *request);
int MPI_Wait(MPI_Request *request, MPI_Status *status);
int MPI_Waitall(int count, MPI_Request array_of_requests[],
                MPI_Status array_of_statuses[]);
int MPI_Waitany(int count, MPI_Request array_of_requests[],
                int *index, MPI_Status *status);
int MPI_Waitsome(int incount, MPI_Request array_of_requests[],
                 int *outcount, int array_of_indices[],
                 MPI_Status array_of_statuses[]);
int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status);
int MPI_Testall(int count, MPI_Request array_of_requests[],
                int *flag, MPI_Status array_of_statuses[]);
int MPI_Iprobe(int source, int tag, MPI_Comm comm, int *flag, MPI_Status *status);
int MPI_Get_count(const MPI_Status *status, MPI_Datatype datatype, int *count);

/* ============================================================
 * Collectives
 * ============================================================ */

int MPI_Barrier(MPI_Comm comm);
int MPI_Ibarrier(MPI_Comm comm, MPI_Request *request);
int MPI_Ibcast(void *buffer, int count, MPI_Datatype datatype,
               int root, MPI_Comm comm, MPI_Request *request);
int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype,
              int root, MPI_Comm comm);
int MPI_Reduce(const void *sendbuf, void *recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm);
int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm);
int MPI_Reduce_scatter(const void *sendbuf, void *recvbuf,
                       const int recvcounts[], MPI_Datatype datatype,
                       MPI_Op op, MPI_Comm comm);
int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
               void *recvbuf, int recvcount, MPI_Datatype recvtype,
               int root, MPI_Comm comm);
int MPI_Gatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                void *recvbuf, const int recvcounts[], const int displs[],
                MPI_Datatype recvtype, int root, MPI_Comm comm);
int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                  void *recvbuf, int recvcount, MPI_Datatype recvtype,
                  MPI_Comm comm);
int MPI_Allgatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                   void *recvbuf, const int recvcounts[], const int displs[],
                   MPI_Datatype recvtype, MPI_Comm comm);
int MPI_Alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                 void *recvbuf, int recvcount, MPI_Datatype recvtype,
                 MPI_Comm comm);

/* ============================================================
 * Datatype
 * ============================================================ */

int MPI_Type_contiguous(int count, MPI_Datatype oldtype, MPI_Datatype *newtype);
int MPI_Type_create_struct(int count, const int array_of_blocklengths[],
                           const MPI_Aint array_of_displacements[],
                           const MPI_Datatype array_of_types[],
                           MPI_Datatype *newtype);
int MPI_Type_create_resized(MPI_Datatype oldtype, MPI_Aint lb, MPI_Aint extent,
                            MPI_Datatype *newtype);
int MPI_Type_commit(MPI_Datatype *datatype);
int MPI_Type_free(MPI_Datatype *datatype);
int MPI_Type_size(MPI_Datatype datatype, int *size);
int MPI_Type_get_extent(MPI_Datatype datatype, MPI_Aint *lb, MPI_Aint *extent);
int MPI_Get_address(const void *location, MPI_Aint *address);
int MPI_Op_free(MPI_Op *op);

/* ============================================================
 * Misc
 * ============================================================ */

int MPI_Query_thread(int *provided);
int MPI_Get_processor_name(char *name, int *resultlen);
double MPI_Wtime(void);
int MPI_Error_string(int errorcode, char *string, int *resultlen);

/* PMPI wrappers (just alias to MPI for now) */
#define PMPI_Init           MPI_Init
#define PMPI_Init_thread    MPI_Init_thread
#define PMPI_Finalize       MPI_Finalize
#define PMPI_Comm_rank      MPI_Comm_rank
#define PMPI_Comm_size      MPI_Comm_size
#define PMPI_Comm_split_type MPI_Comm_split_type

#ifdef __cplusplus
}
#endif

#endif /* MESH_MPI_H */
