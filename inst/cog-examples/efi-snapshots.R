
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


# c6in.4xlarge: 18.8 min max 1.7GB/s, 2*detectCores(),  (31 member ensemble)
# c6in.4xlarge: 43 sec 2 ensemble members
options("mc.cores"=parallel::detectCores()*2)
ensemble = gefs_ensemble()
bench::bench_time({
  gefs_stars(Sys.Date()-33, ensemble =ensemble)
})




# c6in.4xlarge: 24 seconds
options("mc.cores"=2L)
bench::bench_time({
  gefs_to_parquet(Sys.Date()-2,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites = neon_sites())
})



bench::bench_time({
  df <- stars_extract("geavg",
                      reference_datetime = Sys.Date()-32,
                      horizon = gefs_horizon(),
                      bands = gefs_band_numbers(),
                      url_builder = gefs_urls)
})
