
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
                                 endpoint_override = "data.ecoforecast.org"),
           purge = TRUE
           ) {
    
    
  assert_gdal()  
  date <- format(date, "%Y%m%d")
  dest <- fs::dir_create(glue("gefs.{date}"))
  start_time <- paste0(as.Date(date, "%Y%m%d"), " ",cycle,":00:00")
  
  url_vars <- gefs_forecast(date)
  p <- gdal_download(src = url_vars$url, vars = url_vars$vars, dest, threads, gdal_ops)
  
  ns <- neon_coordinates()
  fc <- neon_extract(dest, ns = ns) |> 
    dplyr::mutate(start_time = start_time)
  path <- glue::glue("noaa/neon/gefs/{nice_date}/{date}-{cycle}.parquet")
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
  horizon2 <- stringr::str_pad(seq(240,840,by=6), 3, pad="0")
  horizon <- c(horizon1, horizon2)

  ensemble <-  paste0("gep", stringr::str_pad(1:n_ensemble, 2, pad="0"))
  ensemble <- c("geavg","gec00", ensemble)
  
  cases <- expand.grid(horizon, ensemble, cycle, date) |> 
    stats::setNames(c("horizon", "ensemble", "cycle", "date")) |>
    dplyr::filter(!(ensemble == "gec00" & as.numeric(as.character(horizon)) > 384)) |>
    dplyr::filter(!(cycle != "00" & as.numeric(as.character(horizon)) > 384)) |> 
    dplyr::filter(as.numeric(as.character(horizon)) <= max_horizon) |> 
    dplyr::mutate(vars = ifelse(horizon != "000",
                                "-b 57 -b 63 -b 64 -b 67 -b 68 -b 69 -b 78 -b 79",
                                "-b 57 -b 63 -b 64 -b 67 -b 68")) |> 
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
  gdal <- paste("gdal_translate", 
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
  p <- processx::run("bash", shell)
  
  unlink(shell)
  invisible(p)
}  


assert_gdal <- function() {
  x <- processx::run("gdalinfo", "--version")
  version <- gsub("GDAL (\\d\\.\\d\\.\\d), .*", "\\1", x$stdout)
  stopifnot(utils::compareVersion(version, "3.4.0") >=0 )
}

  