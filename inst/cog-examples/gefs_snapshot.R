
library(gdalcubes)
devtools::load_all()
vis4cast::ignore_sigpipe()
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

# v11 2017-01-01 - 2020-10-01

dates <- seq(as.Date("2021-01-01"), as.Date("2022-01-01"), by=1)
s3 <- gefs_s3_dir("stage1")
bench::bench_time({
  gefs_to_parquet(dates, path = s3)
})
