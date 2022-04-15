library(tidyverse)
library(processx)
library(glue)
library(terra)
source("R/gefs.R")
source("R/neon.R")

# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
dates <- seq(Sys.Date(), Sys.Date()-1, length.out=2)

# s3 <- arrow::s3_bucket("drivers", endpoint_override =  "minio.thelio.carlboettiger.info")
s3 <- arrow::s3_bucket("drivers", endpoint_override =  "data.ecoforecast.org")

bench::bench_time({
map(dates, noaa_gefs, cycle="00", threads=560, s3=s3, gdal_ops="")
})

s3$ls("noaa/neon/gefs")
path <- s3$path("noaa/neon/gefs")
df <- arrow::open_dataset(path)
df |> filter(start_time > as.Date("2022-04-14")) |> collect()
