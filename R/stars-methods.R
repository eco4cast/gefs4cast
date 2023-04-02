
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

