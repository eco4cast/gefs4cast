library(tidyverse)
library(terra)
source("R/gefs.R")
source("R/neon.R")

gefs_tif <- function(
    horizon = "000", # 000:384 hrs ahead
    base = "https://minio.carlboettiger.info/drivers/noaa/tif",
    date = "2022-04-16",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
    res = "0p50" # half 0.50 degree resolution
) {
  glue::glue("/vsicurl/{base}/{date}/",
             "ge{NN}.t{cycle}z.{set}.{res}.f{horizon}.tif")
}


## every 6 hrs, 35 days out, all ensemble members
horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")
ensemble <-  paste0("p", stringr::str_pad(1:30, 2, pad="0"))
cases <- expand.grid(horizon, ensemble) |> 
  setNames(c("horizon", "ensemble")) |>  rowwise() |> 
  mutate(url = gefs_tif(horizon, NN=ensemble))
src <- cases$url




ns <- neon_coordinates()

bench::bench_time({
  stack <- rast(src)
  stack |> 
    terra::extract(ns) |> 
    efi_format(ns = ns)
})




