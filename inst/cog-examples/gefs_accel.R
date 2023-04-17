reference_datetime <- as.Date("2020-11-22")

library(minio)
install_mc()
mc_alias_set("s3",access_key = "", secret_key = "", endpoint = "s3.amazonaws.com")

# No need for /dev/shm on cirrus since network speed < NVME speed
# AWS machines should use /dev/shm for performance
shm <- "/dev/shm/noaa"
shm <- fs::path_expand("~/noaa_shm")

# storage
# shm <- glue::glue("/nvme/noaa-staging/gefs.{date}/{cycle}/atmos/pgrb2ap5/", date = format(reference_datetime, "%Y%m%d"), cycle="00")

bench::bench_time({
  cmd <- glue::glue("mirror s3/noaa-gefs-pds/gefs.{date}/{cycle}/atmos/pgrb2ap5/ {shm}",
                    date = format(reference_datetime, "%Y%m%d"), cycle="00")
  minio::mc(cmd)
})

devtools::load_all()


gdalcubes::gdalcubes_options(parallel=4*parallel::detectCores())
reference_datetime <- as.Date("2020-11-22")

ensemble <- gefs_ensemble()
horizon <-  gefs_horizon()
sites <- neon_sites()

bench::bench_time({
  nonzero_horiz <- lapply(ensemble, function(ens) {
    date_time <- reference_datetime + lubridate::hours(horizon)
    urls <- gefs_urls(ens, reference_datetime, horizon)
  #  urls <- paste0(shm, basename(urls))

    cube <-
      gdalcubes::stack_cube(urls,
                            datetime_values = date_time,
                            band_names = gefs_all_bands())  |> # 5sec
      gdalcubes::select_bands(gefs_bands()) |>
      gdalcubes::extract_geom(sites)
  }) #one ensemble member, full horizon: 2.5 min on cirrus, 20sec on c6in.32xlarge
})
# with local sync -- 1min to sync to /dev/shm, 2 min to run on c6in.32xlarge


bench::bench_time({
zero_horiz <- lapply(ensemble, function(ens) {
  urls <- gefs_urls(ens, reference_datetime, "000")
  urls <- paste0("/dev/shm/noaa/", basename(urls))

  cube0 <-
    gdalcubes::stack_cube(urls,
                          datetime_values = reference_datetime,
                          band_names = gefs_all_bands(zero_horizon = TRUE))|> # 2.5sec
    gdalcubes::select_bands(gefs_bands(TRUE)) |>
    gdalcubes::extract_geom(sites)
})

})



ens <- gefs_ensemble()
horizon <-  gefs_horizon()
date_time <- reference_datetime + lubridate::hours(horizon)
urls <- gefs_urls(ens, reference_datetime, horizon)
sites <- neon_sites()
paths <- gsub("/vsicurl/https://noaa-gefs-pds.s3.amazonaws.com",
              "s3/noaa-gefs-pds", urls)


local_urls <- gsub("noaa-gefs-pds.s3.amazonaws.com/",
                   "minio.carlboettiger.info/noaa-staging/", urls)
#x <- stars::read_stars(local_urls)


bench::bench_time({ # NVME: 39s thelio, 23 sec cirrus
  cube <-
    gdalcubes::stack_cube(local_urls,
                          datetime_values = date_time,
                          band_names = gefs_all_bands())  |> # 5sec
    gdalcubes::select_bands(gefs_bands()) |>
    gdalcubes::extract_geom(sites)
})

bench::bench_time({
  cube <-
    gdalcubes::stack_cube(urls,
                          datetime_values = date_time,
                          band_names = gefs_all_bands())  |> # 5sec
    gdalcubes::select_bands(gefs_bands()) |>
    gdalcubes::extract_geom(sites)
})
