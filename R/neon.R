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


neon_extract <- function(dest, ns = neon_coordinates(), start_time) { 
  fs::dir_ls(dest, glob= "*.tif") |>
    terra::rast() |> 
    terra::extract(ns) |> 
    efi_format(ns = ns, start_time = start_time)
  
}


efi_format <- function(fc_by_site, ns = neon_coordinates(), start_time) {
  
  tifs <- fs::dir_ls(dest, glob= "*.tif")
  fc_by_site <- terra::rast(tifs) |> terra::extract(ns)
  
  ## Parse layer name information
  layer_names <- names(fc_by_site)
  layers <- tibble::tibble(L = layer_names) |> 
    tibble::rowid_to_column() |> 
    arrange(L)
  
  ensemble <-  layers |> 
    dplyr::count(L) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(layers = paste(L, 1:n, sep=":", collapse = ",")) |>
    tidyr::separate_rows(layers, sep=",") |>  
    dplyr::mutate(ensemble = stringi::stri_extract(layers, regex="\\d+$")) |>
    arrange(L, ensemble)|> 
    bind_cols(select(layers,-L)) |> 
    arrange(rowid)
  
  stopifnot( identical(ensemble$L, names(fc_by_site)) )
  
  rownames(fc_by_site) <- rownames(ns)
  names(fc_by_site) <- ensemble$layers
  
  fc <- 
    fc_by_site |> 
    tibble::rownames_to_column(var = "site_id") |>
    tidyr::pivot_longer(-site_id, names_to="variable", values_to="predicted") |>
    tidyr::separate(variable, into=c("variable", "height", "horizon", "ensemble"),
                    sep=":", remove = FALSE) |> 
    dplyr::mutate(ensemble = as.integer(ensemble),
                  start_time = start_time,
                  time = start_time + get_hour(horizon))
  
  fc
}

get_hour <- function(horizon) {
  x <- stringi::stri_extract(horizon, regex="^\\d+")
  lubridate::hours(as.integer(x))
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

