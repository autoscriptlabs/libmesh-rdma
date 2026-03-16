/* mpio.h stub — HDF5 parallel I/O header */
#ifndef MPIO_H
#define MPIO_H
#include "mpi.h"
typedef MPI_Comm MPI_File;
typedef MPI_Info MPI_File_info;
#define MPI_MODE_RDONLY    1
#define MPI_MODE_WRONLY    2
#define MPI_MODE_RDWR      4
#define MPI_MODE_CREATE    8
#define MPI_SEEK_SET       0
#define MPI_SEEK_CUR       1
#define MPI_SEEK_END       2
typedef long MPI_Offset;
#endif
