/*
 * btl_mesh.c — OpenMPI BTL plugin using libmesh-rdma
 * Bypasses NVIDIA firmware gatekeeping with manual QP setup.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/proc.h"
#include "opal/class/opal_bitmap.h"
#include "opal/datatype/opal_convertor.h"

#include "mesh_rdma.h"

#define BTL_MESH_MAX_PEERS      64
#define BTL_MESH_MAX_SEND_SIZE  524287
#define BTL_MESH_EAGER_LIMIT    4096
#define BTL_MESH_MAX_PENDING    256

#define BTL_MESH_NUM_BUFS 64
#define BTL_MESH_BUF_SIZE 524288

typedef struct {
    mesh_rdma_conn_t    *conn;
    mesh_rdma_mr_t      *send_mr;
    mesh_rdma_mr_t      *recv_mr;
    char                *send_pool;     /* NUM_BUFS * BUF_SIZE */
    char                *recv_pool;     /* NUM_BUFS * BUF_SIZE */
    int                  send_head;     /* next send slot */
    int                  recv_head;     /* next recv to post */
    int                  connected;
    opal_proc_t         *proc;
    uint64_t             wr_id_counter;
} mca_btl_mesh_endpoint_t;

typedef struct {
    mca_btl_base_descriptor_t   base;
    mca_btl_base_segment_t      segment;
    mca_btl_mesh_endpoint_t    *endpoint;
    char                       *buf;
    size_t                      size;
} mca_btl_mesh_descriptor_t;

typedef struct {
    mca_btl_mesh_descriptor_t  *des;
    mca_btl_base_tag_t          tag;
    int                         active;
} mesh_pending_t;

static mesh_rdma_ctx_t         *g_ctx = NULL;
static int                      g_nic_idx = -1;
static mca_btl_mesh_endpoint_t  g_endpoints[BTL_MESH_MAX_PEERS];
static int                      g_num_endpoints = 0;
static void                    *g_listen_state = NULL;
static mesh_pending_t           g_pending[BTL_MESH_MAX_PENDING];

/* Forward declarations */
static mca_btl_base_module_t mesh_btl_module;
static int mesh_btl_progress(void);
mca_btl_base_component_3_0_0_t mca_btl_mesh_component;

/* ---- BTL functions ---- */

static int mesh_btl_add_procs(struct mca_btl_base_module_t *btl,
    size_t nprocs, struct opal_proc_t **procs,
    struct mca_btl_base_endpoint_t **endpoints,
    struct opal_bitmap_t *reachable)
{
    (void)btl;
    for (size_t i = 0; i < nprocs; i++) {
        if (procs[i] == opal_proc_local_get()) {
            endpoints[i] = NULL;
            continue;
        }

        mesh_rdma_handle_t *rh = NULL;
        size_t sz = 0;
        int rc;
        OPAL_MODEX_RECV(rc, &mca_btl_mesh_component.btl_version,
                         &procs[i]->proc_name, (void **)&rh, &sz);
        if (rc != OPAL_SUCCESS || sz != sizeof(mesh_rdma_handle_t)) {
            endpoints[i] = NULL;
            continue;
        }

        mca_btl_mesh_endpoint_t *ep = &g_endpoints[g_num_endpoints];
        ep->proc = procs[i];

        /* Rendezvous: lower rank accepts, higher rank connects.
         * Both sides published listen handles via modex.
         * Compare process names to determine role. */
        opal_process_name_t my_name = opal_proc_local_get()->proc_name;
        int i_am_lower = (my_name.vpid < procs[i]->proc_name.vpid);

        if (i_am_lower) {
            /* I listen, peer connects to me */
            fprintf(stderr, "[btl_mesh] Accepting from peer %zu (I am rank %u)\n",
                    i, my_name.vpid);
            if (mesh_rdma_accept(g_ctx, g_listen_state, &ep->conn) != 0) {
                fprintf(stderr, "[btl_mesh] Accept failed for peer %zu\n", i);
                endpoints[i] = NULL;
                continue;
            }
        } else {
            /* I connect to peer's listen handle */
            fprintf(stderr, "[btl_mesh] Connecting to peer %zu (I am rank %u)\n",
                    i, my_name.vpid);
            if (mesh_rdma_connect(g_ctx, rh, &ep->conn) != 0) {
                fprintf(stderr, "[btl_mesh] Connect failed for peer %zu\n", i);
                endpoints[i] = NULL;
                continue;
            }
        }

        ep->send_pool = calloc(1, BTL_MESH_MAX_SEND_SIZE);
        ep->recv_pool = calloc(1, BTL_MESH_MAX_SEND_SIZE);
        /* Register MR on the NIC that the connection actually uses */
        int conn_nic_idx = 0;
        for (int n = 0; n < mesh_rdma_num_nics(g_ctx); n++) {
            if (mesh_rdma_get_nic(g_ctx, n) == ep->conn->nic) {
                conn_nic_idx = n;
                break;
            }
        }
        fprintf(stderr, "[btl_mesh] Registering MR on NIC %d (%s) pd=%p\n",
                conn_nic_idx, ep->conn->nic->rdma_name,
                (void*)ep->conn->nic->pd);
        fprintf(stderr, "[btl_mesh] QP pd=%p, conn nic pd=%p\n",
                (void*)ep->conn->qp->pd, (void*)ep->conn->nic->pd);
        mesh_rdma_reg_mr(g_ctx, conn_nic_idx, ep->send_pool, BTL_MESH_MAX_SEND_SIZE, &ep->send_mr);
        mesh_rdma_reg_mr(g_ctx, conn_nic_idx, ep->recv_pool, BTL_MESH_MAX_SEND_SIZE, &ep->recv_mr);
        mesh_rdma_post_recv(ep->conn, ep->recv_pool, BTL_MESH_MAX_SEND_SIZE,
                            ep->recv_mr->mr->lkey, 0);

        ep->connected = 1;
        ep->wr_id_counter = 1;
        endpoints[i] = (struct mca_btl_base_endpoint_t *)ep;
        g_num_endpoints++;
        opal_bitmap_set_bit(reachable, i);
        fprintf(stderr, "[btl_mesh] Connected to peer %zu via RDMA\n", i);
    }
    return OPAL_SUCCESS;
}

static int mesh_btl_del_procs(struct mca_btl_base_module_t *btl,
    size_t nprocs, struct opal_proc_t **procs,
    struct mca_btl_base_endpoint_t **endpoints)
{
    (void)btl; (void)procs;
    for (size_t i = 0; i < nprocs; i++) {
        mca_btl_mesh_endpoint_t *ep = (mca_btl_mesh_endpoint_t *)endpoints[i];
        if (!ep) continue;
        if (ep->send_mr) mesh_rdma_dereg_mr(ep->send_mr);
        if (ep->recv_mr) mesh_rdma_dereg_mr(ep->recv_mr);
        free(ep->send_pool); free(ep->recv_pool);
        if (ep->conn) mesh_rdma_close(ep->conn);
        ep->connected = 0;
    }
    return OPAL_SUCCESS;
}

static mca_btl_base_descriptor_t *mesh_btl_alloc(struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *ep, uint8_t order, size_t size, uint32_t flags)
{
    (void)btl; (void)order;
    mca_btl_mesh_endpoint_t *mep = (mca_btl_mesh_endpoint_t *)ep;
    mca_btl_mesh_descriptor_t *des = calloc(1, sizeof(*des));
    if (!des) return NULL;

    des->buf = mep->send_pool;  /* alloc just needs a writable area — send picks the real slot */
    if (size > BTL_MESH_BUF_SIZE) {
        fprintf(stderr, "[btl_mesh] WARN: message %zu > BUF_SIZE %d\n", size, BTL_MESH_BUF_SIZE);
    }
    des->size = size;
    des->endpoint = mep;
    des->segment.seg_addr.pval = des->buf;
    des->segment.seg_len = size;
    des->base.des_segments = &des->segment;
    des->base.des_segment_count = 1;
    des->base.des_flags = flags;
    des->base.order = MCA_BTL_NO_ORDER;
    return &des->base;
}

static int mesh_btl_free(struct mca_btl_base_module_t *btl,
    mca_btl_base_descriptor_t *descriptor)
{
    (void)btl;
    free(descriptor);
    return OPAL_SUCCESS;
}

static mca_btl_base_descriptor_t *mesh_btl_prepare_src(
    struct mca_btl_base_module_t *btl, struct mca_btl_base_endpoint_t *ep,
    struct opal_convertor_t *convertor, uint8_t order,
    size_t reserve, size_t *size, uint32_t flags)
{
    mca_btl_base_descriptor_t *des = mesh_btl_alloc(btl, ep, order, reserve + *size, flags);
    if (!des) return NULL;
    mca_btl_mesh_descriptor_t *mdes = (mca_btl_mesh_descriptor_t *)des;

    if (*size > 0) {
        struct iovec iov;
        uint32_t iov_count = 1;
        size_t max_data = *size;
        iov.iov_base = (char *)mdes->buf + reserve;
        iov.iov_len = max_data;
        opal_convertor_pack(convertor, &iov, &iov_count, &max_data);
        *size = max_data;
    }
    mdes->segment.seg_len = reserve + *size;
    return des;
}

static int mesh_btl_send(struct mca_btl_base_module_t *btl,
    struct mca_btl_base_endpoint_t *ep,
    struct mca_btl_base_descriptor_t *descriptor,
    mca_btl_base_tag_t tag)
{
    (void)btl;
    mca_btl_mesh_descriptor_t *des = (mca_btl_mesh_descriptor_t *)descriptor;
    mca_btl_mesh_endpoint_t *mep = (mca_btl_mesh_endpoint_t *)ep;

    if (!mep || !mep->connected) return OPAL_ERR_UNREACH;

    /* Always copy into a registered send slot —
     * PML may pass buffers outside our MR */
    size_t payload = des->segment.seg_len;
    int slot = mep->send_head % BTL_MESH_NUM_BUFS;
    mep->send_head++;
    char *sbuf = mep->send_pool + slot * BTL_MESH_BUF_SIZE;

    sbuf[0] = (char)tag;
    memcpy(sbuf + 1, des->segment.seg_addr.pval, payload);
    size_t total = payload + 1;

    uint64_t wr_id = mep->wr_id_counter++;
    if (mesh_rdma_post_send(mep->conn, sbuf, total,
                             mep->send_mr->mr->lkey, wr_id) != 0) {
        return OPAL_ERROR;
    }

    for (int i = 0; i < BTL_MESH_MAX_PENDING; i++) {
        if (!g_pending[i].active) {
            g_pending[i].des = des;
            g_pending[i].tag = tag;
            g_pending[i].active = 1;
            break;
        }
    }
    fprintf(stderr, "[btl_mesh] SEND %zu bytes tag=%d\n", total, tag);
    return 1; /* in progress */
}

static mca_btl_base_module_t mesh_btl_module;
static int mesh_btl_progress(void)
{
    int completed = 0;

    for (int e = 0; e < g_num_endpoints; e++) {
        mca_btl_mesh_endpoint_t *ep = &g_endpoints[e];
        if (!ep->connected || !ep->conn) continue;

        struct ibv_wc wc;
        memset(&wc, 0, sizeof(wc));
        int ret = ibv_poll_cq(ep->conn->cq, 1, &wc);
        if (ret <= 0) continue;
        static int dbg2 = 0;
        if (++dbg2 <= 50)
            fprintf(stderr, "[btl_mesh] CQ: status=%d opcode=%d bytes=%u wr_id=%lu\n",
                    wc.status, wc.opcode, wc.byte_len, (unsigned long)wc.wr_id);
        if (wc.status != IBV_WC_SUCCESS) continue;

        completed++;

        if (wc.opcode & IBV_WC_RECV) {
            /* RECEIVE completion — deliver to PML */
            int recv_slot = (int)wc.wr_id;
            char *recv_ptr = ep->recv_pool + recv_slot * BTL_MESH_BUF_SIZE;
            mca_btl_base_tag_t tag = (mca_btl_base_tag_t)recv_ptr[0];
            mca_btl_base_segment_t seg;
            seg.seg_addr.pval = recv_ptr + 1;
            seg.seg_len = wc.byte_len - 1;

            mca_btl_base_descriptor_t rdesc;
            memset(&rdesc, 0, sizeof(rdesc));
            rdesc.des_segments = &seg;
            rdesc.des_segment_count = 1;

            /* Deliver to PML via registered callback */
            mca_btl_base_active_message_trigger[tag].cbfunc(
                &mesh_btl_module, tag, &rdesc,
                mca_btl_base_active_message_trigger[tag].cbdata);

            /* Re-post this receive slot */
            int rslot = (int)wc.wr_id;
            mesh_rdma_post_recv(ep->conn,
                                ep->recv_pool + rslot * BTL_MESH_BUF_SIZE,
                                BTL_MESH_BUF_SIZE,
                                ep->recv_mr->mr->lkey, rslot);
        } else {
            /* SEND completion — invoke send callback */
            for (int i = 0; i < BTL_MESH_MAX_PENDING; i++) {
                if (g_pending[i].active) {
                    mca_btl_mesh_descriptor_t *des = g_pending[i].des;
                    g_pending[i].active = 0;
                    if (des->base.des_cbfunc) {
                        des->base.des_cbfunc(
                            &mesh_btl_module,
                            (struct mca_btl_base_endpoint_t *)ep,
                            &des->base, OPAL_SUCCESS);
                    }
                    break;
                }
            }
        }
    }
    return completed;
}

/* ---- Module definition ---- */

static mca_btl_base_module_t mesh_btl_module = {
    .btl_component = NULL,
    .btl_eager_limit = BTL_MESH_EAGER_LIMIT,
    .btl_rndv_eager_limit = BTL_MESH_EAGER_LIMIT,
    .btl_max_send_size = BTL_MESH_MAX_SEND_SIZE,
    .btl_exclusivity = MCA_BTL_EXCLUSIVITY_LOW + 10,
    .btl_flags = MCA_BTL_FLAGS_SEND | MCA_BTL_FLAGS_SEND_INPLACE,
    .btl_bandwidth = 77000,
    .btl_latency = 1,
    .btl_add_procs = mesh_btl_add_procs,
    .btl_del_procs = mesh_btl_del_procs,
    .btl_alloc = mesh_btl_alloc,
    .btl_free = mesh_btl_free,
    .btl_prepare_src = mesh_btl_prepare_src,
    .btl_send = mesh_btl_send,
    .btl_sendi = NULL,
    .btl_put = NULL,
    .btl_get = NULL,
};

/* ---- Component ---- */

static int mesh_component_open(void) { return OPAL_SUCCESS; }
static int mesh_component_close(void) {
    if (g_ctx) { mesh_rdma_destroy(g_ctx); g_ctx = NULL; }
    return OPAL_SUCCESS;
}
static int mesh_component_register(void) { return OPAL_SUCCESS; }

static mca_btl_base_module_t **mesh_component_init(int *num_btls,
    bool enable_progress_threads, bool enable_mpi_threads)
{
    (void)enable_progress_threads; (void)enable_mpi_threads;

    if (mesh_rdma_init(&g_ctx) != 0) { *num_btls = 0; return NULL; }

    g_nic_idx = -1;
    for (int i = 0; i < mesh_rdma_num_nics(g_ctx); i++) {
        mesh_rdma_nic_t *nic = mesh_rdma_get_nic(g_ctx, i);
        if (nic->ip_addr != 0) { g_nic_idx = i; break; }
    }
    if (g_nic_idx < 0) { mesh_rdma_destroy(g_ctx); g_ctx = NULL; *num_btls = 0; return NULL; }

    mesh_rdma_nic_t *nic = mesh_rdma_get_nic(g_ctx, g_nic_idx);
    fprintf(stderr, "[btl_mesh] Using NIC %s GID index %d\n", nic->rdma_name, nic->gid_index);

    mesh_rdma_handle_t handle;
    if (mesh_rdma_listen(g_ctx, g_nic_idx, &handle, &g_listen_state) != 0) {
        *num_btls = 0; return NULL;
    }

    int _modex_rc;
    OPAL_MODEX_SEND(_modex_rc, OPAL_PMIX_GLOBAL,
                     &mca_btl_mesh_component.btl_version,
                     &handle, sizeof(handle));
    if (_modex_rc != OPAL_SUCCESS)
        fprintf(stderr, "[btl_mesh] MODEX_SEND failed: %d\n", _modex_rc);

    memset(g_endpoints, 0, sizeof(g_endpoints));
    memset(g_pending, 0, sizeof(g_pending));

    mesh_btl_module.btl_component = (mca_btl_base_component_t *)&mca_btl_mesh_component;

    mca_btl_base_module_t **modules = malloc(2 * sizeof(mca_btl_base_module_t *));
    modules[0] = &mesh_btl_module;
    modules[1] = NULL;
    *num_btls = 1;
    return modules;
}

mca_btl_base_component_3_0_0_t mca_btl_mesh_component = {
    .btl_version = {
        MCA_BTL_DEFAULT_VERSION("mesh"),
        .mca_open_component = mesh_component_open,
        .mca_close_component = mesh_component_close,
        .mca_register_component_params = mesh_component_register,
    },
    .btl_data = {
        .param_field = MCA_BASE_METADATA_PARAM_CHECKPOINT,
    },
    .btl_init = mesh_component_init,
    .btl_progress = mesh_btl_progress,
};
