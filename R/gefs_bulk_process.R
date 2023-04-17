
#' gefs_bulk_process
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
#' @param path path to local directory or S3 bucket (see [arrow::write_dataset()])
#' @param partitioning partitioning structure used in writing the parquet data
#' @param shm local directory with at least 100 GB temporary storage space
#' @inheritParams grib_extract
#' @export
gefs_bulk_process <- function(dates = Sys.Date() - 1L,
                            path = "gefs_parquet",
                            ensemble = gefs_ensemble(),
                            bands = gefs_bands(),
                            sites = neon_sites(),
                            horizon = gefs_horizon(),
                            all_bands = gefs_all_bands(),
                            url_builder = gefs_urls,
                            cycle = "00",
                            partitioning = c("reference_datetime",
                                             "site_id"),
                            shm = tempfile("noaa-staging")) {

  assert_gdal_version("3.4.0")
  family <- "ensemble"
  if(any(grepl("gespr", ensemble))) family <- "spread"

  lapply(dates, function(date) {
    message(date)
    tryCatch({

      stash_local(date, shm, cycle)
      bench::bench_time({
        nonzero_horiz <- lapply(ensemble, function(ens) {
          date_time <- reference_datetime + lubridate::hours(horizon)
          urls <- gefs_urls(ens, reference_datetime, horizon)
          urls <- paste0(shm, basename(urls))
          #urls <- gsub(glue::glue("noaa-gefs-pds.s3.amazonaws.com/gefs.{date}/{cycle}/atmos/pgrb2ap5/", date = format(reference_datetime, "%Y%m%d"), cycle="00"), "minio.carlboettiger.info/noaa-staging/", urls)
          cube <-
            gdalcubes::stack_cube(urls,
                                  datetime_values = date_time,
                                  band_names = gefs_all_bands()) |>
          gdalcubes::select_bands(gefs_bands()) |>
            gdalcubes::extract_geom(sites)
        }) #one ensemble member, full horizon: 2.5 min on cirrus, 20sec on c6in.32xlarge
      })

      df1 <- nonzero_horiz |>
        efi_format_cubeextract(reference_datetime = reference_datetime,
                               sites = sites,
                               bands = gefs_bands())


      # with local sync -- 1min to sync to /dev/shm, 2 min to run on c6in.32xlarge


      bench::bench_time({
        zero_horiz <- lapply(ensemble, function(ens) {
          urls <- gefs_urls(ens, reference_datetime, "000")
          urls <- paste0(shm, basename(urls))
          cube0 <-
            gdalcubes::stack_cube(urls,
                                  datetime_values = reference_datetime,
                                  band_names = gefs_all_bands(zero_horizon = TRUE))|> # 2.5sec
            gdalcubes::select_bands(gefs_bands(TRUE)) |>
            gdalcubes::extract_geom(sites)
        })

      })

      bench::bench_time({
        df0 <- zero_horiz |>
          efi_format_cubeextract(reference_datetime = reference_datetime,
                                 sites = sites,
                                 bands = gefs_bands(TRUE))
      })

      dplyr::bind_rows(df, df0) |>
        dplyr::mutate(family = family) |>
        arrow::write_dataset(path, partitioning=c("reference_datetime", "site_id"))

      fs::dir_delete(shm)

    },
    error = function(e) warning(paste("date", date, "failed with:\n", e),
                                call.=FALSE),
    finally=NULL)
    invisible(date)
  })

}

stash_local <- function(reference_datetime, shm=tempfile("noaa-staging"), cycle = "00") {
  minio::install_mc()
  minio::mc_alias_set("s3",access_key = "", secret_key = "", endpoint = "s3.amazonaws.com")
  bench::bench_time({
    cmd <- glue::glue("mirror s3/noaa-gefs-pds/gefs.{date}/{cycle}/atmos/pgrb2ap5/ {shm}",
                      date = format(reference_datetime, "%Y%m%d"), cycle="00")
    minio::mc(cmd)
  })
}
