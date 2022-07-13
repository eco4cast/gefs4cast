# Helper utility to check which dates and cycles are missing from the data
# Useful for cron job
check_for_updates <- function(s3) {
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
  
  
  full_dates <- list()
  cycle_dates <- list()
  cycles <- list()
  if(start < avail_day -1 ) {
    # If strictly more than a full day behind, get all records
    full_dates <- seq(start, avail_day-1, by=1)
    cycle_dates <- full_dates
  } else if (start == avail_day - 1) {
    ## we have none of the available day, so get all available cycles:
    full_dates <- avail_day
    cycles <- avail_cycles
    cycle_dates <- avail_day
    
  } else if (start == avail_day) {
    # If we have some of the available day, we need only missing cycles
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
  }
  list(full_dates = full_dates, cycle_dates = cycle_dates, cycles = cycles)
  
}