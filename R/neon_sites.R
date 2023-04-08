#' NEON sites as a simple features point geometry object
#'
#' @param crs coordinate reference system to be used for the desired return
#' @return an sf object with coordinates for NEON sites
#' @export
#' @examples
#' \donttest{
#' sites <- neon_sites()
#' }
neon_sites <- function(crs = sf::st_crs(grib_wkt()) ) {
  sites <- readr::read_csv(paste0("https://github.com/eco4cast/",
                                  "neon4cast-noaa-download/",
                                  "raw/master/noaa_download_site_list.csv"),
                           show_col_types = FALSE)
  sf_sites <- sf::st_as_sf(sites,coords=c("longitude", "latitude"),
                           crs = 4326) |>
    tibble::rowid_to_column("FID") |>
    sf::st_transform(crs = crs)
}
