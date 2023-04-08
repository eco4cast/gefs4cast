
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()
vis4cast::ignore_sigpipe()

#options("mc.cores"=parallel::detectCores()) # overload instead

gdalcubes_options(parallel=2*parallel::detectCores())

dates <- seq(as.Date("2021-01-01"), as.Date("2023-04-08"), by=1)
s3 <- cfs_s3_dir("6hrly/00")

bench::bench_time({
  cfs_to_parquet(dates,  horizon = cfs_horizon, path = s3)
})
