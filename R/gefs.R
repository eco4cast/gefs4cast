
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
           name_pattern = "noaa/gefs-v12/stage1/{nice_date}/{cycle}/neon.parquet"
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
  
  path <- glue::glue(name_pattern)
  outfile <- s3$path(path)
  arrow::write_parquet(fc, outfile)
  
  if (purge) { 
    fs::dir_delete(dest)
  }
  
  invisible(p)
}


gefs_filename <- function(horizon, cycle = "00", set = "pgrb2a", NN = "gep01",
                          res = "0p50",   extension = "") {
  glue::glue("{NN}.t{cycle}z.{set}.{res}.f{horizon}{extension}")
}


gefs_url <- function(
    horizon = "000", # 000:384 hrs ahead
    date = "20220314",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "gep01", # p01-p20 replicates, or  "avg"
    res = "0p50", # half 0.50 degree resolution
    base = "https://noaa-gefs-pds.s3.amazonaws.com/"
) {
  glue::glue("/vsicurl/{base}",
             "gefs.{date}/{cycle}/{series}/{set}p5/",
             gefs_filename(horizon, cycle, set, NN, res))
}

## every 6 hrs, 35 days out, all ensemble members
gefs_forecast <- function(date = "20220314",
                          cycle = "00",    # 00, 06, 12, 18 hr issued
                          series = "atmos",
                          set = "pgrb2a", # or pgrb2b for less common vars
                          res = "0p50", # half 0.50 degree resolution,
                          n_ensemble = 30,
                          ens_avg = FALSE,
                          max_horizon = 840,
                          base = "https://noaa-gefs-pds.s3.amazonaws.com/"
) {
  horizon1 <- stringr::str_pad(seq(0,240,by=3), 3, pad="0")
  horizon2 <- stringr::str_pad(seq(246,840,by=6), 3, pad="0")
  horizon <- c(horizon1, horizon2)

  ensemble <-  paste0("gep", stringr::str_pad(1:n_ensemble, 2, pad="0"))
  ensemble <- c("geavg","gec00", ensemble)
  
  cases <- expand.grid(horizon, ensemble) |> 
    stats::setNames(c("horizon", "ensemble")) |>
    dplyr::filter(!(ensemble == "gec00" & as.numeric(as.character(horizon)) > 384)) |>
    dplyr::filter(!(cycle != "00" & as.numeric(as.character(horizon)) > 384)) |> 
    dplyr::filter(as.numeric(as.character(horizon)) <= max_horizon) |> 
    dplyr::mutate(vars = ifelse(horizon != "000",
                                "-b 57 -b 63 -b 64 -b 67 -b 68 -b 69 -b 78 -b 79",
                                "-b 57 -b 64 -b 65 -b 66 -b 67")) |> 
    dplyr::mutate(ens_avg = ens_avg) |> 
    dplyr::filter((ens_avg == TRUE & ensemble == "geavg") | (ens_avg == FALSE & ensemble != "geavg")) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(url = gefs_url(horizon, date, NN=ensemble, cycle)) |> 
    dplyr::select(url, vars)
  cases
}
  

# For info mapping band id numbers to descriptions, see 
# https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2a.0p50.f003.shtml
# could easily generalize this to take compression and output format as options
gdal_download <- function(src, 
                          vars,
                          dest = ".", 
                          threads=70, 
                          gdal_ops = "-co compress=zstd"
                          ) {
  
  GDAL_BIN <- Sys.getenv('GDAL_BIN', "/usr/local/bin/")
  
  gdal <- paste(paste0(GDAL_BIN,
                "gdal_translate"), 
                gdal_ops,
                "-of GTIFF",
                vars,
                src, 
                file.path(dest, paste0(basename(src), ".tif &")))
  groups <- c(seq(1, length(src), by=threads), length(src))
  cmd <- c("#!/bin/bash", "set -e")
  for(i in 1:(length(groups)-1)){
    cmd <- c(cmd, gdal[seq(groups[i],groups[i+1], by=1)], "wait")
  }
  shell <- "src.sh"
  cmd <- c(cmd, "wait", "echo 'Finshed!'")
  readr::write_lines(cmd, shell)
  p <- processx::run("/usr/bin/bash", shell)
  
  unlink(shell)
  invisible(p)
}  


assert_gdal <- function() {
  GDAL_BIN <- Sys.getenv('GDAL_BIN', "/usr/local/bin/")
  
  x <- processx::run(paste0(GDAL_BIN, "gdalinfo"), "--version")
  version <- gsub("GDAL (\\d\\.\\d\\.\\d), .*", "\\1", x$stdout)
  stopifnot(utils::compareVersion(version, "3.4.0") >=0 )
}

  