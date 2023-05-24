#' cfs_to_parquet
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. '01', '02', '03', '04')
#' @param path path to local directory or S3 bucket (see [arrow::write_dataset()])
#' @param bands named vector of bands to extract
#' @param ensemble list of ensembles
#' @param sites sf object of sites
#' @param horizon vector of horizons (in hours, as integer values), or as constructor
#' function.  CFS requires a dynamic constructor since horizon varies by reference date
#' and ensemble.
#' @param all_bands vector of all band names, needed for
#' `[gdalcubes::stack_cube()]`
#' @param url_builder function that constructs URLs to access grib files.
#' must be a function of horizon, ens, reference_datetime, cycle, and any
#' optional additional arguments.
#' @param cycle cycle indicating start time when forecast was generated
#' (i.e. "00", "06", "12", or "18" hours into reference_datetime)
#' @param family statistical family, e.g. 'ensemble' or 'normal'
#' @param partitioning partitioning structure used in writing the parquet data
#' @export
#'
cfs_to_parquet <- function(dates = Sys.Date() - 1L,
                           path = "cfs_parquet",
                           ensemble = cfs_ensemble(),
                           bands = cfs_bands(),
                           sites = neon_sites() |> sf::st_shift_longitude(),
                           horizon = cfs_horizon,
                           all_bands = cfs_all_bands(),
                           url_builder = cfs_urls,
                           cycle = "00",
                           family = "ensemble",
                           partitioning = c("reference_datetime",
                                            "site_id")) {

  assert_gdal_version("3.6.0") # needed for grb2 over vsicurl
  grib_to_parquet(dates, path, ensemble, bands, sites, horizon,
                  all_bands, url_builder, cycle, family, partitioning)
}


#' cfs_s3_dir
#'
#' @param product target folder, e.g. "6hrly/00" for cycle 00, 6-hrly product
#' @inheritParams gefs_s3_dir
#' @export
cfs_s3_dir <- function(product,
                       path = "neon4cast-drivers/noaa/cfs/",
                       endpoint = "https://sdsc.osn.xsede.org",
                       bucket = paste0("bio230014-bucket01/", path, product)) {

  ignore_sigpipe()

  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3$CreateDir(bucket)
  s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
  s3_dir
}

#' cfs metadata
#'
#' table of data bands accessed from CFS forecasts,
#' along with descriptions
#' @export
cfs_metadata <- function() {
  system.file("extdata/cfs-selected-bands.csv",
              package="gefs4cast") |>
    readr::read_csv(show_col_types = FALSE)
}

cfs_bands <- function() {
  meta <- cfs_metadata()
  out <- meta$band
  names(out) <- meta$id
  out
}

cfs_url <- function(horizon,
                    ens = 1,
                    reference_datetime=Sys.Date()-2,
                    cycle = "00",
                    interval = "6hrly") {

   base = "https://noaa-cfs-pds.s3.amazonaws.com"
   datetime <- reference_datetime + lubridate::hours(horizon)
   file <-
     glue::glue("flxf",
                format(datetime, "%Y%m%d"),
                format(datetime, "%H"),
                ".0",
                "{ens}.",
                format(reference_datetime, "%Y%m%d"),
                cycle, ".grb2")

   ref_date <- format(reference_datetime, "%Y%m%d")
   glue::glue("/vsicurl/{base}/cfs.{ref_date}/",
              "{cycle}/{interval}_grib_0{ens}/{file}")
}

cfs_horizon <- function(ens = 1, reference_datetime = Sys.Date()-1) {
  month_horizon <- 7
  if(as.integer(ens) > 1){
    month_horizon <- 4
  }

  # note modulus math required!
  reference_datetime <- lubridate::as_date(reference_datetime)
  month_remainder <- (lubridate::month(reference_datetime) + month_horizon)
  m <-  month_remainder %% 12
  y <- lubridate::year(reference_datetime) + month_remainder %/% 12
  end <- lubridate::as_date(paste(y, m, "01", sep="-"))
  days <- end - reference_datetime
  units(days) <- "days"
  horizon_hours = 24 * as.integer(days)
  interval_hours=6

  if(horizon_hours <= 0) return(0)
  as.character(seq(0, horizon_hours, by = interval_hours))
}


cfs_urls <- function(ens = 1,
                     reference_datetime=Sys.Date()-4,
                     horizon = NULL, # ignored, calculated dynamically
                     cycle = "00",
                     interval = "6hrly") {

  dynamic_horizon = cfs_horizon(ens, reference_datetime)
  urls <- dynamic_horizon |>
    purrr::map_chr(cfs_url,
           ens = ens,
           reference_datetime = reference_datetime,
           cycle = cycle,
           interval = interval)

  urls
}

cfs_ensemble <- function() as.character(1:4)
cfs_all_bands <- function() paste0("x", 1:101)

# extracted from example grb2 file:
# "https://noaa-cfs-pds.s3.amazonaws.com/cfs.20181031/00/6hrly_grib_01/flxf2018103100.01.2018103100.grb2"

cfs_bbox <- function(){
  # WKT extracted from grib (not really necessary)
  wkt <- grib_wkt()
  bbox <- sf::st_bbox(
    c(xmin=-0.4687493,
      #ymin=-90.2493158,
      ymin=-90,
      xmax=359.5307493,
      ymax=89.7506842),
    crs = wkt)
}

## Use AWS to check which files exist
cfs_horizon_days <- function(ens=1, reference_datetime = Sys.Date()-2) {
  ref_datetime <- format(reference_datetime, "%Y%m%d")
  s3 <-
    glue::glue("noaa-cfs-pds/cfs.{ref_datetime}/00/6hrly_grib_0{ens}/") |>
    arrow::s3_bucket(anonymous=TRUE)
  all_gribs <- s3$ls()
  flxf <- all_gribs[ grepl("flxf\\d{10}.*\\.grb2$", all_gribs) ]
  horizons <- gsub(paste0("flxf(\\d{10})\\.0", ens,
                          "\\.(\\d{10})\\.grb2"), "\\1", flxf) |>
    lubridate::as_datetime(format="%Y%m%d%H")
  diff <- horizons - lubridate::as_datetime(reference_datetime)
  units(diff) <- "days"
  max(diff)
}
