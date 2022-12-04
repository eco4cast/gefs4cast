
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
#' @param date date forecast is issued
#' @param cycle hour forecast is issued (00, 06, 12, 18)
#' @param threads parallel processes to run
#' @param gdal_ops options to GDAL (e.g. compression)
#' @param max_horizon maximum horizon
#' @param dest destination directory
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
           threads = 70,
           gdal_ops = "-co compress=zstd",
           max_horizon = 840
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
    
    url_vars <- gefs_forecast(date, cycle=cycle, max_horizon = max_horizon)
    p <- gdal_download(src = url_vars$url, vars = url_vars$vars, dest, threads, gdal_ops)
  }
