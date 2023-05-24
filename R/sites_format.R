

sites_format <- function(sites) {
  vars <- colnames(sites)
  if (!("FID" %in% vars)) {
    sites <- tibble::rowid_to_column(sites, "FID")
  }

  if (!("site_id" %in% vars)) {
    sites <- tibble::rownames_to_column(sites, "site_id")
  }

  if (!inherits(sites, "sf")) {
    longitude <- grepl("^[Ll]ong", vars)
    latitude <- grepl("^[Ll]at", vars)
    sites <- sf::st_as_sf(sites, coords=c(longitude,latitude), crs=4326)
  }
  sites
}


