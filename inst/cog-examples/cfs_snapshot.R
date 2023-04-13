
library(gdalcubes)
library(gefs4cast)
devtools::load_all()
vis4cast::ignore_sigpipe()
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

dates <- seq(as.Date("2018-10-31"), as.Date("2018-11-01"), by=1)
s3 <- cfs_s3_dir("6hrly/00")

bench::bench_time({
  cfs_to_parquet(dates,  horizon = cfs_horizon, path = s3)
})
