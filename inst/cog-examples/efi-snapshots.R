# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs-v12/stage-stats/reference_datetime/site


library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

options("mc.cores"=2L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-2,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})




options("mc.cores"=parallel::detectCores()) # 5.4min w/ 24 cores
bench::bench_time({
  gefs_to_parquet(Sys.Date()-13, ensemble = gefs_ensemble())
})


bench::bench_time({
  mclapply(gefs_ensemble, gefs_stars_extract,
           reference_datetime = Sys.Date() - 15,
           mc.cores = options("mc.cores", 1L))
})
