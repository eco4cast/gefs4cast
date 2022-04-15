library(tidyverse)
library(processx)
library(glue)
library(terra)

threads <- 140 # can probably be at least 140, maybe 280, depending on free RAM


gefs <- function(
    horizon = "000", # 000:384 hrs ahead
    base = "https://noaa-gefs-pds.s3.amazonaws.com/",
    date = "20220314",
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
# https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2a.0p50.f003.shtml
gdal <- paste("gdal_translate -co compress=zstd  -of GTIFF -b 63 -b 64 -b 65 -b 66 -b 69 -b 78 -b 79", src, paste0(basename(src), ".tif &"))
groups <- seq(1, length(src), by=threads)

cmd <- ""
for(i in 1:(length(groups)-1)){
  cmd <- c(cmd, gdal[seq(groups[i],groups[i+1], by=1)], "wait")
}
cmd <- c(cmd, "wait", "echo 'Finshed!'")
readr::write_lines(cmd, "src.sh")
bench::bench_time(processx::run("bash", "src.sh"))


# NEON site longitude/latitudes
neon_sites <- readr::read_csv("https://github.com/eco4cast/neon4cast-noaa-download/raw/master/noaa_download_site_list.csv")
ns <- neon_sites |> select(longitude, latitude) |> as.matrix()
rownames(ns) <- neon_sites$site_id
ext <- terra::ext(terra::vect(ns))

tif <- fs::dir_ls( glob= "*.tif")
stack <- rast(tif)
site <- extract(stack, ns)

## Reshape into EFI standard
layer_names <- names(site)
n_ensembles <- length(layer_names) / length(unique(layer_names))
ensemble_id <- rep(1:n_ensembles, each = length(unique(layer_names)))

fc <- 
  site |> transpose() |> setNames(neon_sites$site_id) |>  as_tibble() |> 
  unnest(cols=everything()) |> mutate(variable = layer_names, ensemble = ensemble_id) |>
  pivot_longer(-all_of(c("variable", "ensemble")), names_to = "site_id", values_to = "predicted") |>
  tidyr::separate(variable, into=c("variable", "height", "horizon"), sep=":")

# optionally, add NEON lat/lon back?
# fc <- left_join(fc, neon_sites)
date <- "20220314"
cycle <- "00"
outfile <- glue::glue("noaa-gefs-{date}-{cycle}.csv.gz")
write_csv(fc, outfile)

## parquet is much faster to write and about half the size
outfile <- glue::glue("noaa-gefs-{date}-{cycle}.parquet")
arrow::write_parquet(fc, outfile)



## spatial operations: too many file connections to process all ensembles in one pass

gefs_filename <- function(
    horizon = "000", # 000:384 hrs ahead
    date = "20220314",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
    res = "0p50" # half 0.50 degree resolution
) {
  glue::glue("ge{NN}.t{cycle}z.{set}.{res}.f{horizon}.tif")
}
horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")

fs::dir_create(date)
NN = "p01"
rep <- map_chr(horizon, gefs_filename, NN=NN)
stack_rep <- rast(rep)

stack2 <- stack_rep |> crop(ext) 
stack_rep |> crop(ext) |> terra::writeCDF(glue("{date}/{cycle}-{NN}.nc"), overwrite=TRUE) # 72 MB
stack_rep |> crop(ext) |> terra::writeRaster(glue("{date}/{cycle}-{NN}.tif"), gdal=c("COMPRESS=ZSTD", "TFW=YES")) # 43 MB

fs::dir_info(date)

