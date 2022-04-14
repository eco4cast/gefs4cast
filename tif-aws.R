library(tidyverse)
library(terra)

gefs <- function(
    horizon = "000", # 000:384 hrs ahead
    base = "https://minio.carlboettiger.info/shared-data/noaa-gefs/",
    date = "20220318",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
    res = "0p50" # half 0.50 degree resolution
) {
  glue::glue("/vsicurl/{base}",
             "gefs.{date}/{cycle}/{series}/{set}p5/",
             "ge{NN}.t{cycle}z.{set}.{res}.f{horizon}.tif")
}
## every 6 hrs, 35 days out, all ensemble members
horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")
ensemble <-  paste0("p", stringr::str_pad(1:30, 2, pad="0"))
cases <- expand.grid(horizon, ensemble) |> 
  setNames(c("horizon", "ensemble")) |>
  rowwise() |> 
  mutate(url = gefs(horizon, NN=ensemble))
src <- cases$url

bench::bench_time({
  stack <- rast(src)
  site <- extract(stack, c(230.5,5))

  # pivot
  nms <- names(site)
  x <- site |> unname() |> transpose() |> setNames(c("site1", "site2")) |>
    as_tibble() |> mutate(variable = nms, V1 = as.numeric(V1), V2 = as.numeric(V2))
  # split
  x <- x |> 
    tidyr::separate(variable, into=c("variable", "height", "horizon"), sep=":") |>
    tidyr::separate(horizon, into=c("horizon", "ensemble"), sep=" fcst\\.?")

})