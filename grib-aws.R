library(tidyverse, quietly = TRUE)
library(processx, quietly = TRUE)
library(glue, quietly = TRUE)
library(terra, quietly = TRUE)
source("R/gefs.R")
source("R/neon.R")

readRenviron("~/.Renviron")
# Set upload destination
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

endpoint <- "data.ecoforecast.org"
s3 <- arrow::s3_bucket("drivers", endpoint_override = endpoint)



# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth

threads <- 200
#end <- Sys.Date()-1
#start <-as.Date("2022-05-30") #as.Date("2021-01-01")
#dates <- seq(start, end, by=1)

bench::bench_time({
  p1 <-  map(cycle, 
               map(dates, noaa_gefs, cycle="00", 
                   threads=threads, s3=s3, gdal_ops="")
  )
})


cycle <- c("12", "18")
horizon = 6
bench::bench_time({
p1 <-  map(cycle, 
           function(cy) {
             map(dates, noaa_gefs, cycle=cy, max_horizon = horizon,
                 threads=threads, s3=s3, gdal_ops="")
           }
  )
})

