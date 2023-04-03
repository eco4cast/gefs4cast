
#' gefs_s3_dir
#'
#' Helper function that returns a path to the EFI S3 directory
#' @param product product code, e.g. stage1
#' @param path path inside bucket
#' @param endpoint endpoint url
#' @param bucket bucket name
#' @return s3 bucket object (an arrow S3 SubTreeFileSystem object)
#' @export
gefs_s3_dir <- function(product,
                        path = "neon4cast-drivers/noaa/gefs-v12/",
                        endpoint = "https://sdsc.osn.xsede.org",
                        bucket = paste0("bio230014-bucket01/", path, product))
  {

  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
  s3_dir
}


#' gefs_to_parquet
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
#' @param path path to local directory or S3 bucket (see [arrow::write_dataset()])
#' @param partitioning partitioning structure used in writing the parquet data
#' @inheritParams grib_extract
#' @export
gefs_to_parquet <- function(dates = Sys.Date() - 1L,
                            path = "gefs_parquet",
                            ensemble = gefs_ensemble(),
                            bands = gefs_bands(),
                            sites = neon_sites(),
                            horizon = gefs_horizon(),
                            all_bands = gefs_all_bands(),
                            url_builder = gefs_urls,
                            cycle = "00",
                            partitioning = c("reference_datetime",
                                             "site_id")) {

  family <- "ensemble"
  if(any(grepl("gespr", ensemble))) family <- "spread"
  grib_to_parquet(dates, path, ensemble, bands, sites, horizon, all_bands,
                  url_builder, cycle, partitioning)
}

grib_to_parquet <- function(dates = Sys.Date() - 1L,
                            path = "noaa",
                            ensemble,
                            bands,
                            sites = neon_sites(),
                            horizon,
                            all_bands,
                            url_builder,
                            cycle = "00",
                            partitioning = c("reference_datetime",
                                             "site_id")) {
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
      efi_format_cubeextract(date = date, sites = sites) |>
      dplyr::mutate(family = family) |>
      arrow::write_dataset(path,
                           partitioning = partitioning)
    }, error = function(e) warning(paste("date", date, "failed")),
                                   finally=NULL)

  invisible(date)
  })

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
                                   reference_datetime,
                                   sites = neon_sites(),
                                   bands = gefs_bands()) {
  sites_df <- sites |>
    tibble::as_tibble() |>
    dplyr::select("FID", "site_id")
  vars <- names(bands)
  FID <- datetime <- time <- NULL # globalVariables has side effect?

  df <-
    purrr::list_rbind(dfs, names_to = "parameter") |>
    tibble::as_tibble() |>
    dplyr::inner_join(sites_df, by = "FID") |>
    dplyr::rename({bands}) |>
    dplyr::mutate(datetime = lubridate::as_datetime(time)) |>
    dplyr::select(-FID) |>
    tidyr::pivot_longer(vars,
                        names_to = "variable", values_to = "prediction") |>
    dplyr::mutate(reference_datetime = lubridate::as_date(reference_datetime),
                  horizon = datetime - lubridate::as_datetime(reference_datetime))
  df
}
