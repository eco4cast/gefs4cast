#' gefs_pseudo_measures
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
#' @param path path to local directory or S3 bucket (see [arrow::write_dataset()])
#' @param bands named vector of bands to extract
#' @param ensemble list of ensembles
#' @param sites sf object of sites
#' @param horizon vector of horizons (in hours, as integer values), or as constructor
#' function.  CFS requires a dynamic constructor since horizon varies by reference date
#' and ensemble.  000 horizon is added automatically
#' @param all_bands vector of all band names, needed for
#' `[gdalcubes::stack_cube()]`
#' @param url_builder function that constructs URLs to access grib files.
#' must be a function of horizon, ens, reference_datetime, cycle, and any
#' optional additional arguments.
#' @param cycles list of cycle indicating start time when forecast was generated
#' (i.e. "00", "06", "12", or "18" hours into reference_datetime)
#' @param partitioning partitioning structure used in writing the parquet data
#' @export
#'
gefs_pseudo_measures <- function(dates = Sys.Date() - 1L,
                                 path = "gefs_pseudo",
                                 ensemble = gefs_ensemble(),
                                 bands = gefs_bands(),
                                 sites = neon_sites(),
                                 horizon = c("003", "006"),
                                 all_bands = gefs_all_bands(),
                                 url_builder = gefs_urls,
                                 cycles = c("00", "06", "12", "18"),
                                 partitioning = c("reference_datetime")
                                 ) {

  # N.B. partitioning on site_id is broken in arrow 11.x
  gdalcubes_cloud_config()
  assert_gdal_version("3.4.0")
  family <- "ensemble"
  if(any(grepl("gespr", ensemble))) family <- "spread"

  dates_groups <- split(dates, ceiling(seq_along(dates)/30))
  lapply(dates_groups, function(dates) {
    message(paste(min(dates), "---", max(dates)))
    tryCatch({

    df0 <- megacube_extract(dates,
                            ensemble = ensemble,
                            horizon = "000",
                            sites = sites,
                            bands = gefs_bands(TRUE),
                            all_bands = gefs_all_bands(TRUE),
                            url_builder = url_builder,
                            cycles = cycles)
    df1 <- megacube_extract(dates,
                            ensemble = ensemble,
                            horizon = horizon,
                            sites = sites,
                            bands = gefs_bands(),
                            all_bands = gefs_all_bands(),
                            url_builder = url_builder,
                            cycles = cycles)

    dplyr::bind_rows(df1, df0) |>
      dplyr::mutate(family = family) |>
      arrow::write_dataset(path, partitioning=partitioning,
                           max_partitions = 1024 * 32)

  },
  error = function(e) warning(paste("date", date, "failed with:\n", e),
                              call.=FALSE),
  finally=NULL)
  })
  invisible(dates)
}


