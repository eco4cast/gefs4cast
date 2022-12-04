
# library(glue)
# library(stringr)
# library(processx)
# library(dplyr)
# library(readr)
# library(stats)
# library(arrow)

#' Stream NOAA GEFS GRIB to parquet
#'
#' 
#' @param date date forecast is issued
#' @param cycle hour forecast is issued (00, 06, 12, 18)
#' @param threads parallel processes to run
#' @param gdal_ops options to GDAL (e.g. compression)
#' @param s3 an S3 bucket address, from [arrow::s3_bucket]
#' @param max_horizon maximum horizon
#' @param purge logical, clear downloaded/converted tif?
#' @param quiet logical, verbose output?
#' @param dest destination directory
#' @param locations where to access a list of download sites (lat/long)
#' @param name_pattern Naming pattern for upload bucket (glue format)
#'
#' @return invisibly, the processx log
#' @export
#'
#' @examplesIf interactive()
#' noaa_gefs()
noaa_gefs <- 
  function(date = Sys.Date(), 
           cycle = "00", 
           threads = 70,
           gdal_ops = "", # "-co compress=zstd"
           s3 = arrow::s3_bucket("drivers", 
                                 endpoint_override = "data.ecoforecast.org"),
           max_horizon = 840,
           purge = TRUE,
           quiet = FALSE,
           dest = ".",
           locations = paste0("https://github.com/eco4cast/neon4cast-noaa-download/",
                                  "raw/master/noaa_download_site_list.csv"),
           name_pattern = "noaa/gefs-v12/stage1/{cycle_int}/{nice_date}/part-0.parquet"
           ) {
    
  if (date < lubridate::as_date("2020-09-25")) {
    stop("Dates earlier than 2020-09-25 are not currently supported")
  }
  
  if (!quiet) {
  message(paste("date:", date))
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
  ns <- neon_coordinates(locations)
  fc <- neon_extract(dest, ns = ns, start_time)
  
  cycle_int <- as.integer(cycle)
  path <- glue::glue(name_pattern)
  outfile <- s3$path(path)
  arrow::write_parquet(fc, outfile)
  
  if (purge) { 
    fs::dir_delete(dest)
  }
  
  invisible(p)
}


