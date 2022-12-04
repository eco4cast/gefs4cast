
# library(glue)
# library(stringr)
# library(processx)
# library(dplyr)
# library(readr)
# library(stats)
# library(arrow)

#' Stream NOAA GEFS GRIB to Cloud Optimized Geotif
#'
#' 
#' @param dest destination directory
#' @param date date forecast is issued
#' @param cycle hour forecast is issued (00, 06, 12, 18)
#' @param ens_avg should we just access the ensemble average instead?
#' @param series atmos series
#' @param set the GRIB dataset, default to common atmospheric data
#' @param res resolution, 0.50 may be the only available at this time
#' @param max_horizon maximum horizon
#' @param threads parallel processes to run
#' @param gdal_ops options to GDAL (e.g. compression)
#'
#' @return invisibly, the processx log
#' @export
#'
#' @examplesIf interactive()
#' gefs_cog()
gefs_cog <- 
  function(dest = ".", 
           date = Sys.Date(), 
           cycle = "00", 
           ens_avg = FALSE,
           series = "atmos",
           set = "pgrb2a", # or pgrb2b for less common vars
           res = "0p50", 
           max_horizon = 840,
           threads = 70,
           gdal_ops = "-co compress=zstd"
  ) {
    
    if (date < lubridate::as_date("2020-09-25")) {
      stop("Dates earlier than 2020-09-25 are not currently supported")
    }

    assert_gdal()
    stopifnot(cycle %in% c("00", "06", "12", "18"))
    if(is.character(date)) date <- as.Date(date)
    date <- format(date, "%Y%m%d")
    
    
    dest <- fs::dir_create(glue::glue(dest,"/gefs.{date}"))
    nice_date <- as.Date(date, "%Y%m%d")
    start_time <- lubridate::as_datetime(paste0(nice_date, " ",cycle,":00:00"))
    
    url_vars <- gefs_forecast(date, 
                              cycle = cycle,  
                              series = series,
                              set = set, # or pgrb2b for less common vars
                              res = res, 
                              max_horizon = max_horizon, 
                              ens_avg = ens_avg)
    p <- gdal_download(src = url_vars$url, vars = url_vars$vars,
                       dest, threads, gdal_ops)
  }
