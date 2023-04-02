gefs_stars_extract <- function(ens,
                              reference_datetime = Sys.Date()-1,
                              horizon = gefs_horizon(),
                              bands = gefs_bands(),
                              cycle = "00",
                              sites = neon_sites(),
                              ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_times <- reference_datetime + lubridate::hours(horizon)

  gefs_extract <- purrr::possibly(function(datetime, quiet=FALSE) {
    urls<- gefs_urls(ens=ens, date=reference_datetime, horizon=horizon, cycle=cycle)
    paste0("/vsicurl/",urls) |>
      stars::read_stars() |>
      select_bands_(bands) |>
      extract_sites_(sites) |>
      dplyr::mutate(parameter = ens,
                    datetime = datetime,
                    reference_datetime = reference_datetime,
                    family="ensemble")
  })

  parallel::mclapply(date_times,
                     gefs_extract,
                     mc.cores = getOption("mc.cores", 1L)
  ) |>
    purrr::list_rbind()

}
