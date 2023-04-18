
library(gdalcubes)
library(gefs4cast)
devtools::load_all()
vis4cast::ignore_sigpipe()
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

dates <- seq(as.Date("2023-01-01"), Sys.Date(), by=1)
s3 <- gefs_s3_dir("stage1")

bench::bench_time({
  gefs_to_parquet(dates, path = s3)
})
