
library(gdalcubes)
devtools::load_all()
vis4cast::ignore_sigpipe()
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

# v11 2017-01-01 - 2020-10-01
Sys.setenv("GEFS_VERSION"="v11")
dates_v11 <- seq(as.Date("2017-02-20"), as.Date("2020-09-23"), by=1)
s3_v11 <- gefs_s3_dir("stage1", path="neon4cast-drivers/noaa/gefs-v11/")

have_dates <- gsub("reference_datetime=", "", s3_v11$ls())

bench::bench_time({
  gefs_to_parquet(dates_v11, path = s3_v11)
})

#dates <- seq(as.Date("2020-09-24"), as.Date("2021-01-01"), by=1)
#s3 <- gefs_s3_dir("stage1")
have_dates <- gsub("reference_datetime=", "", s3$ls())
