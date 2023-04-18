reference_datetime <- as.Date("2020-11-22")

devtools::load_all()

gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

# No need for /dev/shm on cirrus since network speed < NVME speed
# AWS machines should use /dev/shm for performance
shm <- "/dev/shm/noaa"
shm <- "noaa-staging/" # about 16 min on cirrus

stash_local <- function(reference_datetime, shm=tempfile("noaa-staging"), cycle = "00") {
  minio::install_mc()
  minio::mc_alias_set("s3",access_key = "", secret_key = "", endpoint = "s3.amazonaws.com")
  bench::bench_time({
    cmd <- glue::glue("mirror s3/noaa-gefs-pds/gefs.{date}/{cycle}/atmos/pgrb2ap5/ {shm}",
                      date = format(reference_datetime, "%Y%m%d"), cycle="00")
    minio::mc(cmd)
  })
}

ensemble <- gefs_ensemble()
horizon <-  gefs_horizon()
sites <- neon_sites()
shm_local <- TRUE

bench::bench_time({

    # overload time dimension as time + ensemble
    date_time <- reference_datetime + lubridate::hours(horizon)
    urls <- purrr::map(ensemble, gefs_urls, reference_datetime, horizon)|> purrr::list_c()
    url_ensemble <- purrr::map(ensemble, \(i) rep(i, length(horizon))) |> purrr::list_c()
    fake <- as.Date(Sys.Date()+seq_along(urls))
    unfake <- tibble::tibble(ensemble = url_ensemble, time=as.character(fake))

    if(shm_local) urls <- paste0(shm, basename(urls))
    #urls <- gsub(glue::glue("noaa-gefs-pds.s3.amazonaws.com/gefs.{date}/{cycle}/atmos/pgrb2ap5/", date = format(reference_datetime, "%Y%m%d"), cycle="00"), "minio.carlboettiger.info/noaa-staging/", urls)
    df1 <-
      gdalcubes::stack_cube(urls,
                            datetime_values = fake,
                            band_names = gefs_all_bands()) |>
      gdalcubes::select_bands(gefs_bands()) |>
      gdalcubes::extract_geom(sites) |>
      tibble::as_tibble() |>
      dplyr::left_join(unfake) |>
      dplyr::mutate(time = reference_datetime) |>
      list() |>
      efi_format_cubeextract(reference_datetime = reference_datetime,
                             sites = sites,
                             bands = gefs_bands())

})


# with local sync -- 1min to sync to /dev/shm, 2 min to run on c6in.32xlarge

bench::bench_time({
  urls <- purrr::map_chr(ensemble, function(ens) gefs_urls(ens, reference_datetime, "000"))

  if(shm_local) urls <- paste0(shm, basename(urls))

  fake <- as.Date(Sys.Date()+seq_along(ensemble))
  unfake <- tibble::tibble(ensemble = ensemble, time=as.character(fake))
  df0 <-
    gdalcubes::stack_cube(urls, datetime_values = fake,
                          band_names = gefs_all_bands(zero_horizon = TRUE)) |>
    gdalcubes::select_bands(gefs_bands(TRUE)) |>
    gdalcubes::extract_geom(sites) |>
    tibble::as_tibble() |>
    dplyr::left_join(unfake) |>
    dplyr::mutate(time = reference_datetime) |>
    list() |>
    efi_format_cubeextract(reference_datetime = reference_datetime,
                           sites = sites,
                           bands = gefs_bands(TRUE))
})


family = "ensemble"
path = "noaa_gefs"



gefs_ens <- dplyr::bind_rows(df1, df0) |> dplyr::mutate(family = family)
arrow::write_dataset(gefs_ens, path, partitioning=c("reference_datetime", "site_id"))

x <- purrr::list_rbind(list(df1, df0))


