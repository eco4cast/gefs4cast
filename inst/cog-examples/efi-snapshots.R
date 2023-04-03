
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

# c6in.4xlarge: ~ 14 - 24 sec, 4 cores
# cirrus: 47 sec
options("mc.cores"=4L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-31,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})



# thelio: 11.4min w/ 24 cores,
# cirrus: 11.5 min, 4 cores,  (930Mb/s), uses ~ 44 GB RAM at peak
# c6in.4xlarge, 8 cores: 2.8min
options("mc.cores"=8)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-32, ensemble = gefs_ensemble())
})


# c6in.4xlarge 2.7GB/s, 4 cores 14 min (31 member ensemble)
options("mc.cores"=64)
ensemble = c(mean = "geavg", spr = "gespr")
#ensemble = gefs_ensemble()
bench::bench_time({
  gefs_stars(Sys.Date()-22, ensemble =ensemble)
})




# c6in.4xlarge: 24 seconds
options("mc.cores"=1L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-2,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})

