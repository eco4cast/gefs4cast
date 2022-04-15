# library(dplyr)
# library(terra)
# library(readr)
# library(stringr)
# library(purrr)
# library(tibble)
# library(tidyr)

# NEON site longitude/latitudes
neon_coordinates <- function() {
  sites <- readr::read_csv(paste0("https://github.com/eco4cast/neon4cast-noaa-download/",
                         "raw/master/noaa_download_site_list.csv")) 
  ns <- sites |> dplyr::select(longitude, latitude) |> as.matrix()
  rownames(ns) <- sites$site_id
  ns
}  


## Reshape into EFI standard


efi_format <- function(fc_by_site, ns = neon_coordinates()) {
  layer_names <- names(fc_by_site)
 
  # should be more robust way to do this based on file names
  n_ensembles <- length(layer_names) / length(unique(layer_names))
  ensemble_id <- rep(1:n_ensembles, each = length(unique(layer_names)))
  
  fc <- 
    fc_by_site |>
    purrr::transpose() |>
    stats::setNames(rownames(ns)) |>
    tibble::as_tibble() |> 
    tidyr::unnest(cols=everything()) |> 
    dplyr::mutate(variable = layer_names, ensemble = ensemble_id) |>
    tidyr::pivot_longer(-all_of(c("variable", "ensemble")), 
                 names_to = "site_id", values_to = "predicted") |>
    tidyr::separate(variable, 
                    into=c("variable", "height", "horizon"), sep=":")
  fc
}


neon_extract <- function(dest, ns = neon_coordinates()) { 
  tifs <- fs::dir_ls(dest, glob= "*.tif")
  ensemble_id <- gsub(".*gep(\\d\\d).*", "\\1", tifs)
  tifs |>
    terra::rast() |> 
    terra::extract(ns) |> 
    efi_format(ns = ns)
  
}


## Crop to neon area and write out as tif
neon_tifs <- function(dest, ns = neon_coordinates(), n_ensemble=30) {
  
  ext <- terra::ext(terra::vect(ns))
  ensemble <-  paste0("p", stringr::str_pad(1:n_ensemble, 2, pad="0"))
  
  ## FIXME iterate over all NN
  NN <- ensemble[[1]]
  horizon <- stringr::str_pad(seq(6,840,by=6), 3, pad="0")
  rep <- map_chr(horizon, gefs_filename, NN=NN, extension = ".tif")
  
  rast(file.path(dest, rep)) |>
    crop(ext) |> 
    terra::writeRaster(glue("{dest}/{cycle}-{NN}.tif"), 
                       gdal=c("COMPRESS=ZSTD", "TFW=YES")) # 43 MB
  
}

