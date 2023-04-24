## setup
library(gdalcubes)
devtools::load_all()
vis4cast::ignore_sigpipe()
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())


Sys.setenv("GEFS_VERSION"="v12")

dates <- seq(as.Date("2020-09-24"), Sys.Date()-1, by=1)

#dates <- seq(as.Date("2020-09-24"), as.Date("2021-01-01"), by=1)

bench::bench_time({
  s3 <- gefs_s3_dir("pseudo")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_pseudo_measures(missing_dates,  s3)
})

bench::bench_time({
  s3 <- gefs_s3_dir("stage1")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates, path = s3)
})

bench::bench_time({
  s3 <- gefs_s3_dir("stage1-stats")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates,
                  ensemble=c("geavg", "gespr"),
                  path = s3)
})



### v11 snapshots: 2017-01-01 - 2020-10-01
Sys.setenv("GEFS_VERSION"="v11")
dates <- seq(as.Date("2017-02-20"), as.Date("2018-07-26"), by=1)

bench::bench_time({
  s3 <- gefs_s3_dir("pseudo")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_pseudo_measures(missing_dates,  s3, horizon = "006")
})

bench::bench_time({
  s3 <- gefs_s3_dir("stage1")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates, path = s3)
})



Sys.setenv("GEFS_VERSION"="v11.1")
dates <- seq(as.Date("2018-07-27"), as.Date("2020-09-23"), by=1)
bench::bench_time({
  s3 <- gefs_s3_dir("pseudo")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_pseudo_measures(missing_dates,  s3, horizon = "06")
})


bench::bench_time({
  s3 <- gefs_s3_dir("stage1")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates, path = s3)
})









## may not exist for v11?
bench::bench_time({
  s3 <- gefs_s3_dir("stage1-stats")
  have_dates <- gsub("reference_datetime=", "", s3$ls())
  missing_dates <- dates[!(as.character(dates) %in% have_dates)]
  gefs_to_parquet(missing_dates,
                  ensemble=c("geavg", "gespr"),
                  path = s3)
})
