#band 5: tmp (surface) - not used but is automatically included
#band 11: longwave (surface) DLWRF
#band 16: shortwave (surface) DSWRF
#band 31: rain flux (PRATE) (kg/m^2/s)
#band 36: UGRD Wind (10 m)
#band 37:  (missnumbered as 36.2!): VGRD Wind (10 m)
#band 38: Tmp (2m)
#band 39: specific humidity (2 m) SPFH
#band 40: pressure (surface) PRES
# SNOD Snow Depth
# SNOWC Snow % Cover
# PEVPR potential evaporation rate
# TRANS transpiration rate
# VEG vegetation type
# soilW 0-10cm
# soilW 10-40cm
# soilW 40-100cm
# soilW 100-200cm
# TCDC clm total cloud coover (column)
# PWAT clm atmos column precipitable water (kg/m^2)
# TMAX 2m
# TMIN 2m
# EVPS surface direct evaporation from bare soil
# EVCW canopy water evaporation

cfs_bands <- function() {
  c("TMP_srf" = "x5",
    "DLWRF" = "x11",
    "DSWRF" = "x16",
    "PRATE" = "x31",
    "UGRD" = "x36",
    "VGRD" = "x37",
    "TMP_2m" = "x38",
    "SPFH" = "x39",
    "PRES" = "x40")
}

cfs_s3_dir <- function(product,
                        path = "neon4cast-drivers/noaa/cfs/",
                        endpoint = "https://sdsc.osn.xsede.org",
                        bucket = paste0("bio230014-bucket01/", path, product))
{

  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
  s3_dir
}

cfs_url <- function(datetime,
                    ens = 1,
                    reference_datetime=Sys.Date()-2,
                    cycle = "00",
                    interval = "6hrly") {

   base = "https://noaa-cfs-pds.s3.amazonaws.com"
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


cfs_horizon <- function(horizon_hours = 24 * 200L, interval_hours=6) {
  lubridate::hours(seq(interval_hours, horizon_hours, by = interval_hours))
}

cfs_urls <- function(ens = 1,
                     reference_datetime=Sys.Date()-2,
                     horizon = cfs_horizon(),
                     cycle = "00",
                     interval = "6hrly") {
  lubridate::as_datetime(reference_datetime) + horizon |>
    purrr::map_chr(cfs_url, ens, reference_datetime, cycle, interval)
}

cfs_grib_collection <- function(ens,
                                reference_datetime = Sys.Date()-1,
                                horizon = cfs_horizon(),
                                cycle = "00",
                                interval="6hrly",
                                ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)
  urls <- cfs_urls(ens, reference_datetime, horizon, cycle, ...)
  date_time <- reference_datetime + cfs_horizon()
  gdalcubes::stack_cube(urls,
                        datetime_values = date_time,
                        band_names = cfs_bands)
}

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

assert_gdal_version <- function(version = "3.6.0") {
  gdal_version <- sf:::CPL_gdal_version()
  if(!compareVersion(gdal_version, version)>=0) {
    stop(paste("gdal v", version,
               "is required, but detected gdal v", gdal_version), call. = FALSE)
  }
}
