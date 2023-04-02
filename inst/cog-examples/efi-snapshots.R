# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs-v12/stage-stats/reference_datetime/site


library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

# thelio: 5.4min w/ 24 cores,
# cirrus: 11.5 min, 4 cores,  (930Mb/s), uses ~ 44 GB RAM at peak
# c6in.4xlarge, 8 cores: 2.8min
options("mc.cores"=8)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-29, ensemble = gefs_ensemble())
})

# c6in.4xlarge 2.7GB/s, 4 cores 39.6 min (geavg/gespr members)
options("mc.cores"=8)
ensemble = c(mean = "geavg", spr = "gespr")
ensemble = gefs_ensemble()
bench::bench_time({
  dfs <- parallel::mclapply(ensemble,
                            gefs_stars_extract,
           reference_datetime = Sys.Date() - 20,
           mc.cores = getOption("mc.cores", 1L))
})

# c6in.4xlarge 24 seconds
options("mc.cores"=8L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-2,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})

