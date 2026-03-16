# libmesh-rdma + libmesh-mpi

**RDMA networking for consumer GPU clusters — no managed switch required.**

NVIDIA's ConnectX firmware refuses to establish RDMA connections without a managed InfiniBand switch. This library bypasses that restriction using RC Queue Pairs with TCP-bootstrapped connection management, enabling full RDMA on direct-connect RoCE topologies.

## What's here

### libmesh-rdma — The RDMA transport layer
- RC QP connections with TCP handshake over management network
- Automatic NIC discovery and subnet-aware routing
- GID-based addressing (bypasses ARP, which fails on direct-connect RoCE)
- **Proven: 77 GB/s bandwidth, 11.6μs RTT** on ConnectX-7

### libmesh-mpi — Complete MPI implementation over RDMA
- 55 MPI functions — everything WarpX/AMReX needs
- Ring topology with relay for non-adjacent ranks
- Point-to-point: Send, Recv, Isend, Irecv, Wait, Test
- Collectives: Allreduce, Bcast, Barrier, Gather, Alltoall, etc.
- Tag matching with unexpected message queue
- **75KB shared library. No dependencies except libibverbs.**

## Hardware tested

- 4x NVIDIA DGX Spark (Grace Blackwell GB10, 128GB unified memory each)
- ConnectX-7 NICs in direct-connect ring topology (no switch)
- CUDA 13.0, Ubuntu 24.04, aarch64

## Quick start

### Build
```bash
make                  # builds libmesh_rdma.so
cd mpi && make        # builds libmesh_mpi.so
```

### Test RDMA connectivity
```bash
# Node A
./tests/test_pingpong -s

# Node B
./tests/test_pingpong -c <node-a-rdma-ip>
```

### Run WarpX over RDMA
```bash
cmake -S /path/to/warpx -B build \
    -DMPI_C_INCLUDE_DIRS=$(pwd)/mpi \
    -DMPI_C_LIBRARIES=$(pwd)/mpi/libmesh_mpi.so \
    -DMPI_CXX_INCLUDE_DIRS=$(pwd)/mpi \
    -DMPI_CXX_LIBRARIES=$(pwd)/mpi/libmesh_mpi.so \
    -DCMAKE_C_FLAGS="-I$(pwd)/mpi" \
    -DCMAKE_CXX_FLAGS="-I$(pwd)/mpi" \
    -DCMAKE_CUDA_FLAGS="-I$(pwd)/mpi" \
    -DWarpX_OPENPMD=OFF -DWarpX_COMPUTE=CUDA -DWarpX_DIMS=3

./mpi/mesh_mpirun -np 2 -hosts node-a,node-b python3 your_simulation.py
```

## How it works

### The problem
Every MPI stack (OpenMPI, MPICH+UCX) uses UD (Unreliable Datagram) to bootstrap connections. UD requires `ibv_create_ah()`, which does ARP resolution. On direct-connect RoCE without a managed switch, ARP doesn't resolve across subnets. Every MPI stack fails.

### The solution
RC (Reliable Connected) Queue Pairs embed the destination address directly in `ibv_modify_qp()` — no ARP needed. We bootstrap over TCP on the management network, exchange QP handles, then establish RC connections using GID-based routing. The firmware accepts RC connections because address resolution happens in the QP transition, not in a separate AH object.

## Performance

| Metric | Result |
|--------|--------|
| RDMA bandwidth | 77.24 GB/s |
| RDMA latency | 11.6 μs RTT |
| MPI library size | 75 KB |
| WarpX step time | ~25 ms/step (after warmup) |

## Environment variables

| Variable | Description |
|----------|-------------|
| `MESH_MPI_RANK` | Process rank (set by mesh_mpirun) |
| `MESH_MPI_SIZE` | World size (set by mesh_mpirun) |
| `MESH_MPI_CTRL_ADDR` | Rank 0 management IP:port |
| `MESH_MPI_MGMT_IF` | Management interface (default: enP7s7) |

## Related

- [nccl-mesh-plugin](https://github.com/autoscriptlabs/nccl-mesh-plugin) — NCCL transport for ML inference over RDMA. Together these cover the full GPU computing stack: NCCL for ML, MPI for science.

## Why this matters

The managed switch costs $15,000-50,000. This library makes it cost $0. The hardware was always capable. The restriction was artificial.

## License

MIT
