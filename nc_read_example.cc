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

  printf("read: chunkSize=%zu, numCells=%zu, numSteps=%zu, filename=%s\n", chunkSize, numCells, numSteps, filename);

  int ncId;
  nce(nc_open(filename, NC_NOWRITE, &ncId));

  int varId;
  nce(nc_inq_varid(ncId, "var", &varId));

  size_t size, nelems;
  float preemption;
  nce(nc_get_var_chunk_cache(ncId, varId, &size, &nelems, &preemption));
  printf("default chunk cache: size=%zu, nelems=%zu, preemption=%g\n", size, nelems, preemption);
  size = 4 * numCells; // one float field at one time step
  nelems = 1000;
  preemption = 0.5;
  nce(nc_set_var_chunk_cache(ncId, varId, size, nelems, preemption));
  printf("set chunk cache: size=%zu, nelems=%zu, preemption=%g\n", size, nelems, preemption);

  {
    std::vector<float> var(numCells, 0.0f);
    for (size_t i = 0; i < numSteps; ++i)
      {
        size_t start[2] = {i, 0}, count[2] = {1, numCells};
        nce(nc_get_vara_float(ncId, varId, start, count, var.data()));
      }
  }

  nce(nc_close(ncId));

  printf("Max mem: %zu MB\n", getPeakRSS() / (1024 * 1024));
  
  return 0;
}
