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
reference_datetime <- as.Date("2020-11-30")
ens <- gefs_ensemble()[[1]]
horizon <- c("000", gefs_horizon())
date_time <- reference_datetime + lubridate::hours(horizon)
urls <- gefs_urls(ens, reference_datetime, horizon)
bands <- gefs_all_bands()
band_list <- rep(bands, length(urls)-1)
cube <- gdalcubes::stack_cube(urls,
                              datetime_values = date_time,
                              band_names = gefs_all_bands())

url0 <- gefs_urls(ens, reference_datetime, "000")
cube0 <- gdalcubes::stack_cube(url0,
                              datetime_values = reference_datetime,
                              band_names = gefs_all_bands(zero_horizon = TRUE))


