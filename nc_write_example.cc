#include "netcdf.h"

#include <vector>
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <sys/resource.h>

#if defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
#endif

size_t getPeakRSS(void)
{
  struct rusage rusage;
  getrusage( RUSAGE_SELF, &rusage );
#if defined(__APPLE__) && defined(__MACH__)
  return (size_t)rusage.ru_maxrss;
#else
  return (size_t)(rusage.ru_maxrss * 1024L);
#endif
}

static void
nce(int istat)
{
  if (istat != NC_NOERR)
    {
      fprintf(stderr, "%s\n", nc_strerror(istat));
      exit(-1);
    }
}

int
main(void)
{
  //const char *filename = "testdata.nc";
  const char *filename = "file://testdata.zarr#mode=zarr,file";
  constexpr size_t chunkSize = 262144; // 256k
  constexpr size_t numCells = 50 * chunkSize;
  constexpr size_t numSteps = 360;

  printf("write: chunkSize=%zu, numCells=%zu, numSteps=%zu, filename=%s\n", chunkSize, numCells, numSteps, filename);

  int ncId;
  nce(nc_create(filename, NC_CLOBBER | NC_NETCDF4, &ncId));
  int oldfill;
  nce(nc_set_fill(ncId, NC_NOFILL, &oldfill));

  int dims[2];
  nce(nc_def_dim(ncId, "time", numSteps, &dims[0]));
  nce(nc_def_dim(ncId, "cells", numCells, &dims[1]));

  int varId;
  nce(nc_def_var(ncId, "var", NC_FLOAT, 2, dims, &varId));

  size_t chunks[2] = {1, chunkSize};
  nc_def_var_chunking(ncId, varId, NC_CHUNKED, chunks);

  int shuffle = 0, deflate = 1, level = 3;
  nce(nc_def_var_deflate(ncId, varId, shuffle, deflate, level));

  size_t size, nelems;
  float preemption;
  nce(nc_get_var_chunk_cache(ncId, varId, &size, &nelems, &preemption));
  printf("default chunk cache: size=%zu, nelems=%zu, preemption=%g\n", size, nelems, preemption);
  size = 4 * numCells; // one float field at one time step
  nelems = 1000;
  preemption = 0.5;
  nce(nc_set_var_chunk_cache(ncId, varId, size, nelems, preemption));
  printf("set chunk cache: size=%zu, nelems=%zu, preemption=%g\n", size, nelems, preemption);

  nce(nc_enddef(ncId));

  {
    std::vector<float> var(numCells, 0.0f);
    for (size_t i = 0; i < numSteps; ++i)
      {
        size_t start[2] = {i, 0}, count[2] = {1, numCells};
        nce(nc_put_vara_float(ncId, varId, start, count, var.data()));
      }
  }

  nce(nc_close(ncId));

  printf("Max mem: %zu MB\n", getPeakRSS() / (1024 * 1024));
  
  return 0;
}
