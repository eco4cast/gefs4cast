
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()
#vis4cast::ignore_sigpipe()
#options("mc.cores"=parallel::detectCores())

# c6in.4xlarge:
# cirrus: 5 min all 128 cores 2x overload or 2
dates <- Sys.Date()-3 # seq(as.Date("2023-04-04"), as.Date("2023-04-06"), by=1)
gdalcubes_options(parallel=2*parallel::detectCores())

s3 <- "cfs_parquet" # cfs_s3_dir("6hrly/00")
bench::bench_time({
  cfs_to_parquet(dates,  horizon = cfs_horizon, path = s3)
})

options("mc.cores"=parallel::detectCores())
bench::bench_time({
  cfs_stars(Sys.Date()-1,  horizon = cfs_horizon)
})




