library(tidyverse)
library(processx)
library(glue)
library(terra)
source("R/gefs.R")
source("R/neon.R")


# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth

threads <- 500
days <- 3
end <- as.Date("2022-04-20") #Sys.Date()-1
start <- end - days
dates <- seq(start, end, length.out=(days+1))

# Set upload destination
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

endpoint <-  "js2.jetstream-cloud.org:8001"
#endpoint <- "minio.carlboettiger.info"
s3 <- arrow::s3_bucket("drivers", endpoint_override = endpoint )


cycle <- c("06", "12", "18")
max_horizon=6
#cycle <- "00"
# Here we go
bench::bench_time({
p1 <-  walk(cycle, function(cy)
    map(dates, noaa_gefs, cycle=cy, max_horizon=6, threads=threads, s3=s3, gdal_ops="")
  )
})

# and 00, all horizons
bench::bench_time({
p2 <-    map(dates, noaa_gefs, cycle="00", threads=threads, s3=s3, gdal_ops="")
})


# confirm data access
s3 <- arrow::s3_bucket("drivers", endpoint_override =  endpoint)

s3$ls("noaa/neon/gefs")
path <- s3$path("noaa/neon/gefs")
df <- arrow::open_dataset(path)
df |> filter(start_time == as.Date("2022-04-20")) 



#s3$ls("noaa/neon/gefs/2022-04-20")
## crop tifs
#date <- dates[[1]]
#dest <- fs::dir_create(glue("gefs.{date}"))
#src <- gefs_forecast(date)
#p <- gdal_download(src, dest, threads=1000, gdal_ops="")

#neon_tifs(dest)
