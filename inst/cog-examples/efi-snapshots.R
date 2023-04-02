# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs-v12/stage-stats/reference_datetime/site


library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

# 5.4min w/ 24 cores, thelio
# 13 min, 4 cores, cirrus
options("mc.cores"=4)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-19, ensemble = gefs_ensemble())
})

options("mc.cores"=parallel::detectCores())
ensemble = c(mean = "geavg", spr = "gespr")
bench::bench_time({
  dfs <- parallel::mclapply(ensemble,
                            gefs_stars_extract,
           reference_datetime = Sys.Date() - 20,
           mc.cores = getOption("mc.cores", 1L))
})


options("mc.cores"=1L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-2,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})

