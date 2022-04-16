library(tidyverse)
library(processx)
library(glue)
library(terra)
source("R/gefs.R")
source("R/neon.R")


# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
threads <- 140
days <- 30
dates <- seq(Sys.Date(), Sys.Date()-days, length.out=days+1)


# Set upload destiation
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
# s3 <- arrow::s3_bucket("drivers", endpoint_override =  "minio.thelio.carlboettiger.info")
s3 <- arrow::s3_bucket("drivers", endpoint_override =  "data.ecoforecast.org")


# Here we go
bench::bench_time({
map(dates, noaa_gefs, cycle="00", threads=threads, s3=s3, gdal_ops="")
})


# confirm data access
s3 <- arrow::s3_bucket("drivers", endpoint_override =  "data.ecoforecast.org")

s3$ls("noaa/neon/gefs")
path <- s3$path("noaa/neon/gefs")
df <- arrow::open_dataset(path)
df |> filter(start_time > as.Date("2022-04-14")) |> collect()



## crop tifs
date <- dates[[1]]
dest <- fs::dir_create(glue("gefs.{date}"))
src <- gefs_forecast(date)
p <- gdal_download(src, dest, threads=1000, gdal_ops="")

neon_tifs(dest)
