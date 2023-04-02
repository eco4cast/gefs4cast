gefs_stars_extract <- function(ens,
                              reference_datetime = Sys.Date()-1,
                              horizon = gefs_horizon(),
                              bands = gefs_bands(),
                              cycle = "00",
                              sites = neon_sites(),
                              ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_times <- cfs_horizon(reference_datetime, horizon)

  cfs_extract <- purrr::possibly(function(datetime, quiet=FALSE) {
    cfs_url(datetime, ens, reference_datetime, cycle, interval) |>
      stars::read_stars() |>
      select_bands_(bands) |>
      extract_sites_(sites) |>
      dplyr::mutate(parameter = ens,
                    datetime = datetime,
                    reference_datetime = reference_datetime,
                    family="ensemble")
  })

  parallel::mclapply(date_times,
                     cfs_extract,
                     mc.cores = getOption("mc.cores", 1L)
  ) |>
    purrr::list_rbind()

}
