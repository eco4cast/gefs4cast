library(tidyverse, quietly = TRUE)
library(processx, quietly = TRUE)
library(glue, quietly = TRUE)
library(terra, quietly = TRUE)

source("R/gefs.R")
source("R/neon.R")

# littler-compatible
readRenviron("~/.Renviron")


# Set destination bucket
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
s3 <- arrow::s3_bucket("drivers", endpoint_override = "data.ecoforecast.org")

# most recent date on record
start <- as.Date( max(basename(s3$ls("noaa/neon/gefs"))) ) + 1

# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
threads <- 100
end <- Sys.Date()


p1 <-  map(dates, noaa_gefs, cycle="00", 
           threads=threads, s3=s3, gdal_ops="")

cycle <- c("6", "12", "18")
p1 <-  map(cycle, 
           function(cy) {
             map(dates, noaa_gefs, cycle=cy, max_horizon = 6,
                 threads=threads, s3=s3, gdal_ops="")
           })

