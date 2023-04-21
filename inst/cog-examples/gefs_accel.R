reference_datetime <- as.Date("2020-11-22")

devtools::load_all()

gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

cube_extract <- function(reference_datetime,
                         ensemble = gefs_horizon(),
                         horizon = gefs_horizon(),
                         sites = neon_sites(),
                         bands = gefs_bands(),
                         all_bands = gefs_all_bands(),
                         url_builder = gefs_urls,
                         cycle = "00",
                         shm_local = FALSE) {

    # overload time dimension as time + ensemble
    date_time <- reference_datetime + lubridate::hours(horizon)
    urls <- purrr::map(ensemble, url_builder,
                       reference_datetime, horizon, cycle)|>
      purrr::list_c()
    url_ensemble <- purrr::map(ensemble, \(i) rep(i, length(horizon))) |>
      purrr::list_c()
    fake <- as.Date(Sys.Date()+seq_along(urls))
    unfake <- tibble::tibble(ensemble = url_ensemble, time=as.character(fake))

    if(shm_local) urls <- paste0(shm, basename(urls))

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

bench::bench_time({

df0 <- cube_extract(reference_datetime,
                    ensemble = gefs_ensemble(),
                    horizon = "000",
                    sites = neon_sites(),
                    bands = gefs_bands(TRUE),
                    all_bands = gefs_all_bands(TRUE))
})


