library(tidyverse)

devtools::load_all()

# CFS
reference_datetime <- as.Date("2018-10-31")
ens <- 1
horizon <- cfs_horizon(ens, reference_datetime)
date_time <- reference_datetime + lubridate::hours(horizon)
urls <- cfs_urls(ens, reference_datetime, horizon)

cube <- gdalcubes::stack_cube(urls,
                      datetime_values = date_time,
                      band_names = cfs_all_bands())

out <- cube |>
  gdalcubes::select_bands(cfs_bands()) |>
  gdalcubes::extract_geom(neon_sites() |> sf::st_shift_longitude())


## GEFS
reference_datetime <- as.Date("2020-11-22")
url_builder = gefs_urls
ensemble = gefs_ensemble()[[1]]
family = "ensemble"
date = reference_datetime
cycle = "00"
all_bands <- gefs_all_bands()
bands = gefs_bands()


# full horizon, one ens
gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())

reference_datetime <- as.Date("2020-11-22")
ens <- gefs_ensemble()[[1]]
horizon <-  gefs_horizon()
date_time <- reference_datetime + lubridate::hours(horizon)
urls <- gefs_urls(ens, reference_datetime, horizon)
sites <- neon_sites()

bench::bench_time({
  cube <-
    gdalcubes::stack_cube(urls,
                          datetime_values = date_time,
                          band_names = gefs_all_bands())  |> # 5sec
    gdalcubes::select_bands(gefs_bands()) |>
    gdalcubes::extract_geom(sites)
}) #one ensemble member, full horizon: 2.5 min on cirrus, 20sec on c6in.32xlarge

bench::bench_time({
  cube0 <-
    gdalcubes::stack_cube(gefs_urls(ens, reference_datetime, "000"),
                          datetime_values = reference_datetime,
                          band_names = gefs_all_bands(zero_horizon = TRUE))|> # 2.5sec
    gdalcubes::select_bands(gefs_bands(TRUE)) |>
    gdalcubes::extract_geom(sites)

})

### GEFS
