# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs-v12/stage-stats/reference_datetime/site


library(gdalcubes)
gdalcubes_options(parallel=TRUE)

bench::bench_time({
  gefs_to_parquet(Sys.Date()-1,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})

options("mc.cores"=31L) # 11min w/ 31 cores, 5.32 hrs on 1 core
bench::bench_time({
  gefs_to_parquet(Sys.Date()-1)
})


