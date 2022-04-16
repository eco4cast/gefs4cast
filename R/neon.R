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
                         "raw/master/noaa_download_site_list.csv"), show_col_types = FALSE) 
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
  fs::dir_ls(dest, glob= "*.tif") |>
    terra::rast() |> 
    terra::extract(ns) |> 
    efi_format(ns = ns)
  
}


## Crop to neon area and write back out
neon_tifs <- function(dest,  ns = neon_coordinates()) {
  ext <- terra::ext(terra::vect(ns))
  
  fs::dir_ls(dest, glob= "*.tif") |> 
    map(function(tif){
      rast(tif) |> crop(ext) |> 
        terra::writeRaster(tif, 
                           gdal=c("COMPRESS=ZSTD", "TFW=YES"),
                           overwrite=TRUE)
      
      
    })
  
}

