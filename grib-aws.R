library(tidyverse)
library(processx)
library(glue)
library(terra)
source("R/gefs.R")
source("R/neon.R")

readRenviron("~/.Renviron")
# Set upload destination
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

endpoint <-  "js2.jetstream-cloud.org:8001"
s3 <- arrow::s3_bucket("drivers", endpoint_override = endpoint)



# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth

threads <- 100
end <-  Sys.Date()-2
start <- as.Date("2022-01-01")
dates <- seq(start, end, by=1)
#dates <- as.Date("2022-06-15")

cycle <- c("06", "12", "18")
#max_horizon=6
#cycle <- "00"
# Here we go
bench::bench_time({
p1 <-  walk(cycle, function(cy)
    map(dates, noaa_gefs, cycle=cy,
#        max_horizon=max_horizon,
        threads=threads, s3=s3, gdal_ops="")
  )
})

# and 00, all horizons
bench::bench_time({
p2 <-    map(dates, noaa_gefs, cycle="00", threads=threads, s3=s3, gdal_ops="")
})


# confirm data access
#s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", endpoint_override =  endpoint)
#df <- arrow::open_dataset(s3)
#df |> filter(start_time == as.Date("2022-04-20")) |> head() |> collect()


## unit-test-level
#s3$ls("noaa/neon/gefs/2022-04-20")
## crop tifs
#date <- dates[[1]]
#dest <- fs::dir_create(glue("gefs.{date}"))
#src <- gefs_forecast(date)
#p <- gdal_download(src, dest, threads=1000, gdal_ops="")

#neon_tifs(dest)
