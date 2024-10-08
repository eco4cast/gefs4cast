# These methods seek to imitate gdalcubes in pure `stars`

select_bands_ <- function(r, bands){
  r[,,,unname(bands)]
}

# a `stars` based extract_sites mimicking gdalcubes:
# automatically handle
extract_sites_ <- function(r, sites, variable_dimension = 3) {

  # better to match crs ahead of time, but can do on-the-fly
  if (!identical(sf::st_crs(r), sf::st_crs(sites))) {
    sites <- sf::st_transform(sites, sf::st_crs(r))
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

  if(is.function(horizon)){
    horizon <- horizon(ens, reference_datetime)
  }

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

cfs_band_numbers <- function() {
  meta <- cfs_metadata()
  meta$band_number
}


gefs_band_numbers <- function(zero_horizon=FALSE){
  meta <- gefs_metadata()
  if(zero_horizon) {
    meta <- meta[!is.na(meta$horiz0_number), ]
    meta$Number <- meta$horiz0_number
  }
  meta$Number
}










