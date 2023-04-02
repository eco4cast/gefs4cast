#band 5: tmp (surface) - not used but is automatically included
#band 11: longwave (surface) DLWRF
#band 16: shortwave (surface) DSWRF
#band 31: rain flux (PRATE) (kg/m^2/s)
#band 36: UGRD Wind (10 m)
#band 37:  (missnumbered as 36.2!): VGRD Wind (10 m)
#band 38: Tmp (2m)
#band 39: specific humidity (2 m) SPFH
#band 40: pressure (surface)
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


cfs_horizon <- function(reference_datetime = Sys.Date()-2,
                        horizon=lubridate::days(200)) {

  start <- lubridate::as_datetime(reference_datetime)
  n = 1 + ( horizon / lubridate::hours(6) )
  out <- seq(start, start + horizon, length.out=n)
  stopifnot(out[2] - out[1] == as.difftime(6, units="hours"))
  out[-1] # first horizon has different bands
}

assert_gdal_version <- function(version = "3.6.0") {
  gdal_version <- sf:::CPL_gdal_version()
  if(!compareVersion(gdal_version, version)>=0) {
    stop(paste("gdal v", version,
               "is required, but detected gdal v", gdal_version), call. = FALSE)
  }
}


cfs_stars_extract <- function(ens,
                              reference_datetime = Sys.Date()-1,
                              horizon = lubridate::days(200),
                              cycle = "00",
                              interval="6hrly",
                              sites = neon_sites(),
                              ...) {
  assert_gdal_version()
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_times <- cfs_horizon(reference_datetime, horizon)
  sites <- sites |>
    sf::st_transform(crs = sf::st_crs(grib_wkt())) |>
    dplyr::select("site_id", "geometry")

  bands <- c(31, 36:40)

  cfs_extract <- purrr::possibly(function(datetime, quiet=FALSE) {
    cfs_url(datetime, ens, reference_datetime, cycle, interval) |>
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


cfs_urls <- function(ens = 1,
                     reference_datetime=Sys.Date()-2,
                     horizon = lubridate::days(200),
                     cycle = "00",
                     interval = "6hrly") {
  cfs_horizon(reference_datetime,horizon) |>
    purrr::map_chr(cfs_url, ens, reference_datetime, cycle, interval)
}


cfs_grib_collection <- function(ens,
                                date = Sys.Date()-1,
                                horizon = lubridate::days(200),
                                cycle = "00",
                                interval="6hrly",
                                ...) {
  reference_datetime <- lubridate::as_date(date)
  date_time <- cfs_horizon(reference_datetime, horizon)
  urls <- cfs_urls(ens, reference_datetime, horizon, cycle, interval)
  gdalcubes::stack_cube(urls, datetime_values = date_time, band_names = )
}

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
