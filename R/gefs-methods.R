
#' gefs_to_parquet
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
#' @param path path to local directory or S3 bucket (see [arrow::write_dataset()])
#' @param partitioning partitioning structure used in writing the parquet data
#' @inheritParams grib_extract
#' @export
gefs_to_parquet <- function(dates = Sys.Date() - 1L,
                            path = "gefs_parquet",
                            ensemble = gefs_ensemble(),
                            bands = gefs_bands(),
                            sites = neon_sites(),
                            horizon = gefs_horizon(),
                            all_bands = gefs_all_bands(),
                            url_builder = gefs_urls,
                            cycle = "00",
                            partitioning = c("reference_datetime",
                                             "site_id")) {

  assert_gdal_version("3.4.0")
  family <- "ensemble"
  if(any(grepl("gespr", ensemble))) family <- "spread"
  grib_to_parquet(dates, path, ensemble, bands, sites, horizon, all_bands,
                  url_builder, cycle, family, partitioning)
}


#' gefs_s3_dir
#'
#' Helper function that returns a path to the EFI S3 directory
#' @param product product code, e.g. stage1
#' @param path path inside bucket
#' @param endpoint endpoint url
#' @param bucket bucket name
#' @return s3 bucket object (an arrow S3 SubTreeFileSystem object)
#' @export
#' @examples
#' \donttest{
#' gefs_s3_dir()
#' }
gefs_s3_dir <- function(product = "stage1",
                        path = "neon4cast-drivers/noaa/gefs-v12/",
                        endpoint = "https://sdsc.osn.xsede.org",
                        bucket = paste0("bio230014-bucket01/", path, product))
{

  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
  s3_dir
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

gefs_all_bands <- function(zero_horizon=FALSE){
  if(zero_horizon) return(paste0("band", 1:71))
  paste0("band", 1:85)
}


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
#' @param ... additional parameters (not used, for cross-compatibility only)
#' @return list of horizon values (for cycle 00, gepNN forecasts)
#' @export
gefs_horizon <- function(...) {
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

