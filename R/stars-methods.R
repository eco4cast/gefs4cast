# These methods seek to imitate gdalcubes in pure `stars`

select_bands_ <- function(r, bands){
  r[,,,unname(bands)]
}

# a `stars` based extract_sites mimicking gdalcubes:
# automatically handle
extract_sites_ <- function(r, sites, variable_dimension = 3) {

  # better to match crs ahead of time, but can do on-the-fly
  if (!identical(sf::st_crs(r), sf::st_crs(sites))) {
    sites <- sf::st_transform(sites, st_crs(r))
  }

  # stars::st_extract
  y <- stars::st_extract(r, sf::st_coordinates(sites))

  # variable name parsing from grib idx, not generic
  variables <- stars::st_get_dimension_values(r,variable_dimension)
  variables <- gsub("(\\w+:\\w+):.*", "\\1", variables)
  colnames(y) <- variables

  # format nicely
  out <- sites |>
    dplyr::select("site_id", "geometry") |>
    vctrs::vec_cbind(y) |>
    tibble::as_tibble() # should we leave it as an sf object?

  # convert to EFI 'long' format while variable names are handy
  out |> tidyr::pivot_longer(variables,
                             names_to="variable",
                             values_to="prediction")
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
  stars_to_parquet(dates, path, ensemble, bands, sites, horizon, cycle,
                   url_builder, family, partitioning)
}

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

  lapply(dates, function(date) {
    message(date)
    tryCatch({
      dfs <- lapply(ensemble,
                    stars_extract,
                    reference_datetime = date,
                    bands = bands,
                    sites = sites,
                    horizon = horizon,
                    url_builder = url_builder,
                    cycle = cycle)
      dfs |>
        purrr::list_rbind() |>
        arrow::write_dataset(path,
                             partitioning = partitioning)
    },
    error = function(e) warning(paste("date", date, "failed with:\n", e),
                                call.=FALSE),
    finally=NULL)
    invisible(date)
  })
}

stars_extract <- function(ens,
                          reference_datetime = Sys.Date()-1,
                          horizon,
                          bands,
                          cycle = "00",
                          sites = neon_sites(),
                          url_builder,
                          family = "ensemble",
                          ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)
  extract_possibly <- purrr::possibly(function(h, quiet=FALSE) {
    url_builder(ens = ens,
              reference_datetime = reference_datetime,
              horizon = h,
              cycle = cycle,
              ...) |>
      stars::read_stars() |>
      select_bands_(bands) |>
      extract_sites_(sites) |>
      dplyr::mutate(parameter = ens,
                    datetime = reference_datetime + lubridate::hours(h),
                    reference_datetime = reference_datetime,
                    family=family)
  })

  parallel::mclapply(horizon,
                     extract_possibly,
                     mc.cores = getOption("mc.cores", 1L)
                    ) |>
    purrr::list_rbind()
}

cfs_band_numbers <- function() c(31, 36:40)


gefs_band_numbers <- function(){
  c(57,63,64,67:69,78,79)
}










