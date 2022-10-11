# library(dplyr)
# library(terra)
# library(readr)
# library(stringr)
# library(purrr)
# library(tibble)
# library(tidyr)

# NEON site longitude/latitudes
neon_coordinates <- function(locations) {
  sites <- readr::read_csv(locations,
    show_col_types = FALSE,
    progress = FALSE) 
  ns <- sites |> dplyr::select(longitude, latitude) |> as.matrix()
  rownames(ns) <- sites$site_id
  ns
}  


## Reshape into EFI standard


neon_extract <- function(dest, ns = neon_coordinates(), reference_datetime) { 
  fs::dir_ls(dest, glob= "*.tif") |>
    terra::rast() |> 
    terra::extract(ns) |> 
    efi_format(ns = ns, reference_datetime = reference_datetime)
  
}


efi_format <- function(fc_by_site, ns = neon_coordinates(), reference_datetime) {
  
  ## Parse layer name information
  layer_names <- names(fc_by_site)
  layers <- tibble::tibble(L = layer_names) |> 
    tibble::rowid_to_column() |> 
    dplyr::arrange(L)
  
  ensemble <-  layers |> 
    dplyr::count(L) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(layers = paste(L, 1:n, sep=":", collapse = ",")) |>
    tidyr::separate_rows(layers, sep=",") |>  
    dplyr::mutate(ensemble = stringi::stri_extract(layers, regex="\\d+$")) |>
    dplyr::arrange(L, ensemble)|> 
    dplyr::bind_cols(dplyr::select(layers,-L)) |> 
    dplyr::arrange(rowid)
  
  stopifnot( identical(ensemble$L, names(fc_by_site)) )
  
  rownames(fc_by_site) <- rownames(ns)
  names(fc_by_site) <- ensemble$layers
  
  fc <- 
    fc_by_site |> 
    tibble::rownames_to_column(var = "site_id") |>
    tidyr::pivot_longer(-site_id, names_to="variable", values_to="prediction") |>
    tidyr::separate(variable, into=c("variable", "height", "horizon", "ensemble"),
                    sep=":", remove = FALSE) |> 
    dplyr::mutate(ensemble = as.integer(ensemble),
                  family = "ensemble",
                  reference_datetime = reference_datetime,
                  forecast_valid = horizon,
                  horizon = get_hour(horizon),
                  horizon = as.numeric(horizon, "hours"),
                  horizon = tidyr::replace_na(horizon,0),
                  datetime = reference_datetime + lubridate::hours(horizon)
    ) |>
    dplyr::rename(parameter = ensemble) |> 
    dplyr::left_join(tibble::rownames_to_column(as.data.frame(ns), "site_id"),
              by = "site_id")
  
  fc
}


## FIXME not valid for fluxes.  horizon "6-12" is different from "6-9"
get_hour <- function(horizon) {
  x <- stringi::stri_extract(horizon, regex="^\\d+")
  lubridate::hours(as.integer(x))
}

## Crop to neon area and write back out
neon_tifs <- function(dest,  ns = neon_coordinates()) {
  ext <- terra::ext(terra::vect(ns))
  
  fs::dir_ls(dest, glob= "*.tif") |> 
    lapply(function(tif){
      terra::rast(tif) |> terra::crop(ext) |> 
        terra::writeRaster(tif, 
                           gdal=c("COMPRESS=ZSTD", "TFW=YES"),
                           overwrite=TRUE)
      
      
    })
  
}


globalVariables(c("rowid", "horizon", "latitude", "longitude", "L", "crop",
                  "site_id", "vars", "variable", "n"))