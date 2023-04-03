
gdalcubes_cloud_config <- function() {
  # set recommended variables for cloud access:
  # https://gdalcubes.github.io/source/concepts/config.html#recommended-settings-for-cloud-access
gdalcubes::gdalcubes_set_gdal_config("VSI_CACHE", "TRUE")
gdalcubes::gdalcubes_set_gdal_config("GDAL_CACHEMAX","30%")
gdalcubes::gdalcubes_set_gdal_config("VSI_CACHE_SIZE","10000000")
gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_MULTIPLEX","YES")
gdalcubes::gdalcubes_set_gdal_config("GDAL_INGESTED_BYTES_AT_OPEN","32000")
gdalcubes::gdalcubes_set_gdal_config("GDAL_DISABLE_READDIR_ON_OPEN","EMPTY_DIR")
gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_VERSION","2")
gdalcubes::gdalcubes_set_gdal_config("GDAL_HTTP_MERGE_CONSECUTIVE_RANGES","YES")
gdalcubes::gdalcubes_set_gdal_config("GDAL_NUM_THREADS", "ALL_CPUS")
}

#' grib_extract
#'
#' @param bands numeric vector of bands to extract, see details
#' @param sites sf object of sites to extract
#' @param all_bands names of all bands in grib file
#' @param horizon vector of horizons to extract (in hours)
#' @param url_builder helper function to construct remote URLs.
#' Must be a function of ens, reference_datetime, horizon, cycle, and
#' any named arguments passed via ...
#' @return gdalcubes::image_collection()
#' @details See <https://www.nco.ncep.noaa.gov/pmb/products/gens/> for
#' details on GEFS data, including horizon, cycle, and band information.
#' This function is a simple wrapper around
#' [gdalcubes::extract_geom()]. .
#'
#' @export
grib_extract <-function(ens,
                        reference_datetime = Sys.Date(),
                        bands = gefs_bands(),
                        sites = neon_sites(),
                        horizon = gefs_horizon(),
                        all_bands = gefs_all_bands(),
                        url_builder = gefs_urls,
                        cycle = "00",
                        ...) {

  gdalcubes_cloud_config()
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)

  url_builder(ens, reference_datetime, horizon, cycle, ...) |>
  gdalcubes::stack_cube(datetime_values = date_time,
                        band_names = all_bands) |>
    gdalcubes::select_bands(bands) |>
    gdalcubes::extract_geom(sites)

}




#' mapping of gefs_bands to variable names
#'
#' export
gefs_bands <- function() {
  bands = c("PRES"= "band57",
            "TMP" = "band63",
            "RH" = "band64",
            "UGRD" = "band67",
            "VGRD" = "band68",
            "APCP" = "band69",
            "DSWRF" = "band78",
            "DLWRF" = "band79")
}

gefs_all_bands <- function() paste0("band", 1:85)


# https://www.nco.ncep.noaa.gov/pmb/products/gens/
gefs_urls <- function(ens,
                      reference_datetime = Sys.Date(),
                      horizon = gefs_horizon(),
                      cycle = "00",
                      series = "atmos",
                      resolution = "0p50",
                      base = "https://noaa-gefs-pds.s3.amazonaws.com") {
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)
  gribs <- paste0(
                  base,
                  "/gefs.",format(reference_datetime, "%Y%m%d"),
                  "/", cycle,
                  "/",series,
                  "/pgrb2ap5/",
                  ens,
                  ".t", cycle, "z.",
                  "pgrb2a.0p50.",
                  "f", horizon)
  paste0("/vsicurl/", gribs)
}


#' gefs_horizon
#' @return list of horizon values (for cycle 00, gepNN forecasts)
#' @export
gefs_horizon <- function() {
  c(stringr::str_pad(seq(3,240,by=3), 3, pad="0"),
    stringr::str_pad(seq(246,840,by=6), 3, pad="0"))
}


#' gefs ensemble list
#' @return Generates the strings for the 30 perturbed ensembles and control
#' If only mean and spread are needed, manually pass
#' `c(mean = "geavg", spr = "gespr")`
#' @export
gefs_ensemble <- function() {
  c("gec00", paste0("gep", stringr::str_pad(1:30, 2, pad="0")))
}

#' gefs bounding box
#' @export
gefs_bbox <- function(){
  ## alternately, extract from GRIB
  #date <- as.Date("2022-12-01")
  #grib <- paste0("/vsicurl/",
  #               "https://noaa-gefs-pds.s3.amazonaws.com/gefs.",
  #               format(date, "%Y%m%d"), "/00/atmos/pgrb2ap5/",
  #               "gep01", ".t00z.pgrb2a.0p50.f003")
  #box <- stars::read_stars(grib) |> sf::st_bbox()

  # WKT extracted from grib (not really necessary)
  wkt <- grib_wkt()
  bbox <- sf::st_bbox(c(xmin=-180.25, ymin=-90.25, xmax=179.75, ymax=90.25),
                      crs = wkt)
}

grib_wkt <- function() {
  'GEOGCRS["Coordinate System imported from GRIB file",
    DATUM["unnamed",
        ELLIPSOID["Sphere",6371229,0,
            LENGTHUNIT["metre",1,
                ID["EPSG",9001]]]],
    PRIMEM["Greenwich",0,
        ANGLEUNIT["degree",0.0174532925199433,
            ID["EPSG",9122]]],
    CS[ellipsoidal,2],
        AXIS["latitude",north,
            ORDER[1],
            ANGLEUNIT["degree",0.0174532925199433,
                ID["EPSG",9122]]],
        AXIS["longitude",east,
            ORDER[2],
            ANGLEUNIT["degree",0.0174532925199433,
                ID["EPSG",9122]]]]'
}


#' NEON sites as a simple features point geometry object
#' @return an sf object with coordinates for NEON sites
#' @export
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


# thin wrapper around gdalcubes::write_tif, probably not that useful
# Some workflows may prefer to create COG tifs in a remote cache / object store,
# allowing downstream users to work against the cloud-optimzed geotifs directly.
# When high bandwidth is available, this is especially efficient format for
# subsetting from full spatial data.
grib_to_tif <- function(ens,
                        reference_datetime = Sys.Date(),
                        horizon = gefs_horizon(),
                        cycle = "00",
                        dir = NULL,
                        band = gefs_bands(),
                        all_bands = gefs_all_bands(),
                        creation_options=list(COMPRESS="zstd")) {

  if(is.null(dir)) {
    dir <- fs::dir_create(paste0("gefs.", format(date, "%Y%m%d")))
  }

  gdalcubes_cloud_config()
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)

  url_builder(ens, reference_datetime, horizon, cycle) |>
    gdalcubes::stack_cube(urls,
                          datetime_values = date_time,
                          band_names = all_bands)
  cube |>
    gdalcubes::select_bands(band) |>
    gdalcubes::write_tif(dir,
                         prefix=paste0(ens, ".t00z.pgrb2a.0p50.f"),
                         COG=TRUE,
                         creation_options = creation_options)
}





