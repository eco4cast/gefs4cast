

#' grib_extract
#'
#' @param ens ensemble string
#' @param reference_datetime date forecast is generated
#' @param bands numeric vector of bands to extract, see details
#' @param sites sf object of sites to extract
#' @param all_bands names of all bands in grib file
#' @param horizon vector of horizons to extract (in hours)
#' @param url_builder helper function to construct remote URLs.
#' @param cycle start cycle of forecast
#' @param ... additional arguments for url_builder
#' Must be a function of ens, reference_datetime, horizon, cycle, and
#' any named arguments passed via ...
#' @return gdalcubes::image_collection()
#' @details See <https://www.nco.ncep.noaa.gov/pmb/products/gens/> for
#' details on GEFS data, including horizon, cycle, and band information.
#' This function is a simple wrapper around
#' [gdalcubes::extract_geom()]. .
#'
#' @export
grib_extract <-function(ens,
                        reference_datetime = Sys.Date(),
                        bands = gefs_bands(),
                        sites = neon_sites(),
                        horizon = gefs_horizon(),
                        all_bands = gefs_all_bands(),
                        url_builder = gefs_urls,
                        cycle = "00",
                        ...) {

  gdalcubes_cloud_config()

  if(is.function(horizon)){
    horizon <- horizon(ens, reference_datetime)
  }

  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)

  url_builder(ens, reference_datetime, horizon, cycle, ...) |>
  gdalcubes::stack_cube(datetime_values = date_time,
                        band_names = all_bands) |>
    gdalcubes::select_bands(bands) |>
    gdalcubes::extract_geom(sites)

}




# thin wrapper around gdalcubes::write_tif, probably not that useful
# Some workflows may prefer to create COG tifs in a remote cache / object store,
# allowing downstream users to work against the cloud-optimzed geotifs directly.
# When high bandwidth is available, this is especially efficient format for
# subsetting from full spatial data.
grib_to_tif <- function(ens,
                        reference_datetime = Sys.Date(),
                        horizon = gefs_horizon(),
                        cycle = "00",
                        url_builder = gefs_urls,
                        band = gefs_bands(),
                        all_bands = gefs_all_bands(),
                        dir = NULL,
                        creation_options=list(COMPRESS="zstd")) {

  if(is.null(dir)) {
    dir <- fs::dir_create(paste0("gefs.", format(date, "%Y%m%d")))
  }

  gdalcubes_cloud_config()
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)

  url_builder(ens, reference_datetime, horizon, cycle) |>
    gdalcubes::stack_cube(datetime_values = date_time,
                          band_names = all_bands) |>
    gdalcubes::select_bands(band) |>
    gdalcubes::write_tif(dir,
                         prefix=paste0(ens, ".t00z.pgrb2a.0p50.f"),
                         COG=TRUE,
                         creation_options = creation_options)
}





