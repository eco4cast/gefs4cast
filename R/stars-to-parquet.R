# wrappers around stars_to_parquet with defaults for gefs or cfs

stars_to_parquet <- function(dates,
                             path,
                             ensemble,
                             bands,
                             sites,
                             horizon,
                             cycle = "00",
                             url_builder,
                             family = "ensemble",
                             partitioning = c("reference_datetime",
                                              "site_id"),
                             ...) {

  lapply(dates, function(date) { # loop over reference_datetimes if needed
    message(date)
    tryCatch({
      dfs <- lapply(ensemble, # loop over ensembles
                    stars_extract, # parallel over horizon
                    reference_datetime = date,
                    bands = bands,
                    sites = sites,
                    horizon = horizon,
                    url_builder = url_builder,
                    cycle = cycle)
      dfs |>
        purrr::list_rbind() |>
        dplyr::select(-"geometry") |> # cannot write list-cols to arrow
        arrow::write_dataset(path,
                             partitioning = partitioning)
    },
    error = function(e) warning(paste("date", date, "failed with:\n", e),
                                call.=FALSE),
    finally=NULL)
    invisible(date)
  })
}

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

  assert_gdal_version("3.4.0")
  stars_to_parquet(dates, path, ensemble, bands, sites, horizon, cycle,
                   url_builder, family, partitioning)
}


cfs_stars <- function(dates = Sys.Date()-1,
                      path = "cfs_stars",
                      ensemble = cfs_ensemble(),
                      bands = cfs_band_numbers(),
                      sites = neon_sites() |> sf::st_shift_longitude(),
                      horizon = cfs_horizon,
                      cycle = "00",
                      url_builder = cfs_url,
                      family = "ensemble",
                      partitioning = c("reference_datetime",
                                       "site_id"),
                      ...) {
  assert_gdal_version("3.6.0")

  stars_to_parquet(dates, path, ensemble, bands, sites, horizon, cycle,
                   url_builder, family, partitioning)
}
