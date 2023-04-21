## Replace with megacube_extract
## May need efi-format generalized appropriately
## Then can deprecate all methods here.

#' cube_extract
#'
#' @param reference_datetime date forecast is generated
#' @param ensemble vector of ensembles, string
#' @param bands numeric vector of bands to extract, see details
#' @param sites sf object of sites to extract
#' @param all_bands names of all bands in grib file
#' @param horizon vector of horizons to extract (in hours)
#' @param url_builder helper function to construct remote URLs.
#' @param cycle start cycle of forecast
#' @param shm file path that can be used to replace remote URLs
#'  if data is mirrored to filesystem
#'
#' @export
cube_extract <- function(reference_datetime,
                         ensemble = gefs_ensemble(),
                         horizon = gefs_horizon(),
                         sites = neon_sites(),
                         bands = gefs_bands(),
                         all_bands = gefs_all_bands(),
                         url_builder = gefs_urls,
                         cycle = "00",
                         shm = NULL) {

  # overload time dimension as time + ensemble
  date_time <- reference_datetime + lubridate::hours(horizon)
  urls <- purrr::map(ensemble, url_builder,
                     reference_datetime, horizon, cycle)|>
    purrr::list_c()
  url_ensemble <- purrr::map(ensemble, \(i) rep(i, length(horizon))) |>
    purrr::list_c()
  fake <- as.Date(Sys.Date()+seq_along(urls))
  unfake <- tibble::tibble(ensemble = url_ensemble, time=as.character(fake))

  if(!is.null(shm)) urls <- paste0(shm, basename(urls))

  gdalcubes::stack_cube(urls,
                        datetime_values = fake,
                        band_names = all_bands) |>
    gdalcubes::select_bands(bands) |>
    gdalcubes::extract_geom(sites) |>
    tibble::as_tibble() |>
    dplyr::left_join(unfake, by="time") |>
    dplyr::mutate(time = reference_datetime) |>
    efi_format_cubeextract(reference_datetime = reference_datetime,
                           sites = sites,
                           bands = bands)

}


#' efi_format_cubeextract
#'
#' reformat results from grib_extract to EFI format
#' @param dfs list of tables from grib_extract
#' @param reference_datetime forecast reference_datetime
#' @param sites sites (as sf object)
#' @param bands named vector of band numbers, see [gefs_bands()]
#' @export
efi_format_cubeextract <- function(dfs,
                                   reference_datetime,
                                   sites = neon_sites(),
                                   bands = gefs_bands()) {
  sites_df <- sites |>
    tibble::as_tibble() |>
    dplyr::select("FID", "site_id")
  vars <- names(bands)
  FID <- datetime <- time <- NULL # globalVariables has side effect?

  if(!inherits(dfs, "tbl")){
    dfs <- purrr::list_rbind(dfs, names_to = "parameter") |>
      tibble::as_tibble()
  }

  df <- dfs |>
    dplyr::inner_join(sites_df, by = "FID") |>
    dplyr::rename({bands}) |>
    dplyr::mutate(datetime = lubridate::as_datetime(time)) |>
    dplyr::select(-FID, -time) |>
    tidyr::pivot_longer(vars,
                        names_to = "variable", values_to = "prediction") |>
    dplyr::mutate(reference_datetime = lubridate::as_date(reference_datetime),
                  horizon = datetime - lubridate::as_datetime(reference_datetime))
  df
}



######### should migrate to faster mechansim in cube_extract() ####
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

## Should migrate to cube_extract -- faster method
grib_to_parquet <- function(dates = Sys.Date() - 1L,
                            path = "noaa",
                            ensemble = gefs_ensemble(),
                            bands = gefs_bands(),
                            sites = neon_sites(),
                            horizon = gefs_horizon(),
                            all_bands = gefs_all_bands(),
                            url_builder = gefs_urls,
                            cycle = "00",
                            family = "ensemble",
                            partitioning = c("reference_datetime", "site_id")) {
  lapply(dates, function(date) {
    message(date)
    tryCatch({
      dfs <- parallel::mclapply(ensemble,
                                grib_extract,
                                reference_datetime = date,
                                bands = bands,
                                sites = sites,
                                horizon = horizon,
                                all_bands = all_bands,
                                url_builder = url_builder,
                                cycle = cycle,
                                mc.cores = getOption("mc.cores", 1L))
      dfs |>
        efi_format_cubeextract(reference_datetime = date,
                               sites = sites,
                               bands = bands) |>
        dplyr::mutate(family = family) |>
        arrow::write_dataset(path,
                             partitioning = partitioning)
    },
    error = function(e) warning(paste("date", date, "failed with:\n", e),
                                call.=FALSE),
    finally=NULL)

    invisible(date)
  })

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





