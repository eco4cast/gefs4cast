
library(gdalcubes)
library(gefs4cast)
devtools::load_all()

#gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
gdalcubes::gdalcubes_options(parallel=TRUE)

dates <- seq(as.Date("2018-10-31"), Sys.Date(), by=1)

bench::bench_time({

  s3 <- cfs_s3_dir("6hrly/00")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]

  cfs_to_parquet(dates,  horizon = cfs_horizon, path = s3)
})
