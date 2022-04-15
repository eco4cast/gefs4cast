
# library(glue)
# library(stringr)
# library(processx)
# library(dplyr)
# library(readr)
# library(stats)
# library(arrow)



noaa_gefs <- 
  function(date, 
           cycle = "00", 
           threads = 70,
           gdal_ops = "", # "-co compress=zstd"
           s3 = arrow::s3_bucket("drivers", 
                                 endpoint_override = "data.ecoforecast.org")
           ) {
  date <- format(date, "%Y%m%d")
  dest <- fs::dir_create(glue("gefs.{date}"))
  nice_date <- as.character(as.Date(date, "%Y%m%d"))
  
  src <- gefs_forecast(date)
  p <- gdal_download(src, dest, threads, gdal_ops)
  
  ns <- neon_coordinates()
  fc <- neon_extract(dest, ns = ns) |> mutate(start_time = nice_date)
  path <- glue::glue("noaa/neon/gefs/{nice_date}/{date}-{cycle}.parquet")
  outfile <- s3$path(path)
  arrow::write_parquet(fc, outfile)
  
  fs::dir_delete(dest)
  invisible(p)
}


gefs_filename <- function(horizon, cycle = "00", set = "pgrb2a", NN = "p01",
                          res = "0p50",   extension = "") {
  glue::glue("ge{NN}.t{cycle}z.{set}.{res}.f{horizon}{extension}")
}


gefs_url <- function(
    horizon = "000", # 000:384 hrs ahead
    date = "20220314",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
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
                          base = "https://noaa-gefs-pds.s3.amazonaws.com/"
) {
  horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")
  ensemble <-  paste0("p", stringr::str_pad(1:n_ensemble, 2, pad="0"))
  cases <- expand.grid(horizon, ensemble) |> 
    stats::setNames(c("horizon", "ensemble")) |>
    dplyr::rowwise() |> 
    dplyr::mutate(url = gefs_url(horizon, NN=ensemble))
  cases$url
}
  

# For info mapping band id numbers to descriptions, see 
# https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2a.0p50.f003.shtml
# could easily generalize this to take compression and output format as options
gdal_download <- function(src, 
                          dest = ".", 
                          threads=70, 
                          gdal_ops = "-co compress=zstd"
                          ) {
  gdal <- paste("gdal_translate", 
                gdal_ops,
                "-of GTIFF -b 63 -b 64 -b 65 -b 66 -b 69 -b 78 -b 79",
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
  p <- processx::run("bash", shell)
  
  unlink(shell)
  invisible(p)
}  
  