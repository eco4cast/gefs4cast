library(tidyverse)
library(terra)
library(processx)

threads <- 140*8 # can probably be at least 140, maybe 280, depending on free RAM


gefs <- function(
    horizon = "000", # 000:384 hrs ahead
    base = "https://noaa-gefs-pds.s3.amazonaws.com/",
    date = "20220319",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
    res = "0p50" # half 0.50 degree resolution
) {
  glue::glue("/vsicurl/{base}",
             "gefs.{date}/{cycle}/{series}/{set}p5/",
             "ge{NN}.t{cycle}z.{set}.{res}.f{horizon}")
}
## every 6 hrs, 35 days out, all ensemble members
horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")
ensemble <-  paste0("p", stringr::str_pad(1:31, 2, pad="0"))
cases <- expand.grid(horizon, ensemble) |> 
  setNames(c("horizon", "ensemble")) |>
  rowwise() |> 
  mutate(url = gefs(horizon, NN=ensemble))
src <- cases$url

gdal <- paste("gdal_translate -co compress=zstd -co predictor=2 -co tiled=yes -of GTIFF -b 63 -b 64 -b 65 -b 66 -b 67 -b 68 -b 69 -b 70", src, paste0(basename(src), ".tif &"))
groups <- seq(1, length(src), by=threads)

cmd <- ""
for(i in 1:(length(groups)-1)){
  cmd <- c(cmd, gdal[seq(groups[i],groups[i+1], by=1)], "wait")
}
cmd <- c(cmd, "wait", "echo 'Finshed!'")
readr::write_lines(cmd, "src.sh")
bench::bench_time(processx::run("bash", "src.sh"))


bench::bench_time({
tif <- fs::dir_ls( glob= "*.tif")
stack <- rast(tif)
site <- extract(stack, c(230.5,5))
})
nms <- names(site)
x <- site |> unname() |> transpose() |> setNames(c("V1", "V2")) |> as_tibble() |> mutate(variable = nms, V1 = as.numeric(V1), V2 = as.numeric(V2))
x <- x |> tidyr::separate(variable, into=c("variable", "height", "horizon"), sep=":")
x <- x |> tidyr::separate(horizon, into=c("horizon", "ensemble"), sep=" fcst\\.?")
