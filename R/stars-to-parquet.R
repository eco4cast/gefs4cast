# wrappers around stars_to_parquet with defaults for gefs or cfs

# NB  stars_to_parquet is parallelized over horizon,
# while grib_to_parquet (gdalcubes) is threaded over horizon by gdalcubes,
# and parallelized over ensemble.
gefs_stars <- function(dates = Sys.Date()-1,
                       path = "gefs_stars",
                       ensemble = gefs_ensemble(),
                       bands = gefs_band_numbers(),
                       sites = neon_sites(),
                       horizon = gefs_horizon(),
                       cycle = "00",
                       url_builder = gefs_urls,
                       family = "ensemble",
                       partitioning = c("reference_datetime",
                                        "site_id"),
                       ...) {
  stars_to_parquet(dates, path, ensemble, bands, sites, horizon, cycle,
                   url_builder, family, partitioning)
}


cfs_stars <- function(dates = Sys.Date()-1,
                      path = "cfs_stars",
                      ensemble = cfs_ensemble(),
                      bands = cfs_band_numbers(),
                      sites = neon_sites() |> sf::st_shift_longitude(),
                      horizon = cfs_horizon(),
                      cycle = "00",
                      url_builder = cfs_urls,
                      family = "ensemble",
                      partitioning = c("reference_datetime",
                                       "site_id"),
                      ...) {
  assert_gdal_version()

  stars_to_parquet(dates, path, ensemble, bands, sites, horizon, cycle,
                   url_builder, family, partitioning)
}
