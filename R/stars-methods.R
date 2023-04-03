# These methods seek to imitate gdalcubes in pure `stars`
# The performance of these methods is considerably poorer


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


gefs_band_numbers <- function(){
  c(57,63,64,67:69,78,79)
}


gefs_stars_extract <- function(ens,
                               reference_datetime = Sys.Date()-1,
                               horizon = gefs_horizon(),
                               bands = gefs_band_numbers(),
                               cycle = "00",
                               sites = neon_sites(),
                               family = "ensemble",
                               ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)
  gefs_extract <- purrr::possibly(function(h, quiet=FALSE) {
    gefs_urls(ens = ens,
              reference_datetime = reference_datetime,
              horizon = h,
              cycle = cycle) |>
      stars::read_stars() |>
      select_bands_(bands) |>
      extract_sites_(sites) |>
      dplyr::mutate(parameter = ens,
                    datetime = reference_datetime + lubridate::hours(h),
                    reference_datetime = reference_datetime,
                    family=family)
  })
  parallel::mclapply(horizon,
                     gefs_extract,
                     mc.cores = getOption("mc.cores", 1L)
  ) |>
    purrr::list_rbind()
}


cfs_stars_extract <- function(ens,
                              reference_datetime = Sys.Date()-1,
                              horizon = lubridate::days(200),
                              cycle = "00",
                              sites = neon_sites(),
                              family = "ensemble",
                              interval="6hrly",
                              ...) {
  assert_gdal_version()
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_times <- cfs_horizon(reference_datetime, horizon)
  sites <- sites |>
    sf::st_transform(crs = sf::st_crs(grib_wkt())) |>
    dplyr::select("site_id", "geometry")

  bands <- c(31, 36:40)

  cfs_extract <- purrr::possibly(function(datetime, quiet=FALSE) {
    cfs_url(datetime,
            ens = ens,
            reference_datetime,
            cycle,
            ...) |>
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

