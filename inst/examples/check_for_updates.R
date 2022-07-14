## CRON-job to update the recent GEFS parquet files
## Will pick up from the day after the last date on record

# WARNING: needs >= GDAL 3.4.x
#remotes::install_github("eco4cast/gefs4cast")
library(gefs4cast)
library(purrr)
library(dplyr)

# be littler-compatible
readRenviron("~/.Renviron")

# Set destination bucket
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
s3 <- arrow::s3_bucket("drivers", endpoint_override = "data.ecoforecast.org")

# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
threads <- 100

gefs <- s3$path("noaa/neon/gefs")
have <- gefs$ls()
have_days <- as.Date(basename(have))
start <- max(have_days, na.rm=TRUE)
have_cycles <- basename(gefs$ls(start))

aws <- arrow::s3_bucket("noaa-gefs-pds", anonymous = TRUE)
avail <- aws$ls()
days <- as.Date(gsub("^gefs\\.(\\d{8})", "\\1", avail), "%Y%m%d")
avail_day <- max(days,na.rm=TRUE)
avail_cycles <- basename( aws$ls(avail[which.max(days)]) )

# ick can detect folder before it has data!
# hackish sanity check
A <- aws$ls( paste(avail[which.max(days)], max(avail_cycles), "atmos", "pgrb2ap5", sep="/" ))
B <- aws$ls( paste(avail[which.max(days)-1], max(avail_cycles), "atmos", "pgrb2ap5", sep="/" ))
complete <- length(A) == length(B)
if(!complete) avail_cycles <- avail_cycles[-length(avail_cycles)]



cycles <- c("06", "12", "18")
full_dates <- list()
cycle_dates <- list()

if(start <= avail_day -1 ) {
  # If strictly more than a full day behind, get all records up to day before.
  full_dates <- seq(start, avail_day-1, by=1)
  map(full_dates, noaa_gefs, cycle="00", threads=threads, s3=s3)
  map(cycles, 
      function(cy) {
        map(full_dates, noaa_gefs, cycle=cy, max_horizon = 6,
            threads=threads, s3=s3, gdal_ops="")
      })
  
  ## And also get available records for the current day:
  noaa_gefs(avail_day, cycle="00", threads=threads, s3=s3)
  map(avail_cycles, function(cy) 
    noaa_gefs(avail_day, cycle=cy, threads=threads, s3=s3)
  )
  
  # If we have some of the most recent available day, we need only missing cycles
} else if (start == avail_day) {
  need_cycles <- avail_cycles[!(avail_cycles %in% have_cycles)]
  if(length(need_cycles)==0)
    message("Up to date.")
  else {
    if("00" %in% need_cycles) {
      full_dates <- start
    }
    cycles <- need_cycles[need_cycles != "00"]
    cycle_dates <- start
  }
  
  ## get 00 if it is missing:
  map(full_dates, noaa_gefs, cycle="00", threads=threads, s3=s3)
  ## get the non-00 cycles that are missing
  map(cycles, 
      function(cy) {
        map(cycle_dates, noaa_gefs, cycle=cy, max_horizon = 6,
            threads=threads, s3=s3, gdal_ops="")
      })
  
  
}



