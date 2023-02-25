

gefs_s3_dir <- function(path) {
  endpoint <- "https://sdsc.osn.xsede.org"
  bucket <- paste0("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/", path)
  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
}


#' gefs_to_parquet
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
#' @param s3_dir path to S3 directory
#' @inheritParams grib_extract
gefs_to_parquet <- function(dates,
                            ensemble,
                            s3_dir,
                            sites = neon_sites(),
                            bands = gefs_bands(),
                            cycle = "00",
                            horizon = gefs_horizon()) {

  for(date in dates) {
    dfs <- lapply(ensemble,
                  grib_extract,
                  date = date,
                  sites = sites,
                  bands = bands,
                  cycle = cycle,
                  horizon = horizon)
    dfs |>
      efi_format_cubeextract(date = date, sites = sites) |>
      dplyr::mutate(family = "spread") |>
      arrow::write_dataset(s3_dir,
                           partitioning = c("reference_datetime",
                                            "site_id"))
  }

}


#' efi_format_cubeextract
#'
#' reformat results from grib_extract to EFI format
#' @param dfs list of tables from grib_extract
#' @param date forecast reference_datetime
#' @param sites sites (as sf object)
#' @param bands named vector of band numbers, see [gefs_bands()]
#' @export
efi_format_cubeextract <- function(dfs,
                                   date,
                                   sites = neon_sites(),
                                   bands = gefs_bands()) {

  sites_df <- sites |>
    tibble::as_tibble() |>
    dplyr::select(FID, site_id)
  vars <- names(bands)


  FID <- datetime <- time <- NULL # globalVariables has side effect?

  df <-
    purrr::list_rbind(dfs, names_to = "parameter") |>
    tibble::as_tibble() |>
    dplyr::inner_join(sites_df) |>
    dplyr::rename({bands}) |>
    dplyr::mutate(datetime = lubridate::as_datetime(time)) |>
    dplyr::select(-FID) |>
    tidyr::pivot_longer(vars,
                        names_to = "variable", values_to = "prediction") |>
    dplyr::mutate(reference_datetime = lubridate::as_date(date),
                  horizon = datetime - lubridate::as_datetime(date))
  df
}
