

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
