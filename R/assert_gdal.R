
# 3.6.0 for CFS / grib2
# 3.4.0 for GEFS / grib
assert_gdal_version <- function(version = "3.6.0") {
  gdal_version <- sf::sf_extSoftVersion()[["GDAL"]]
  if(!utils::compareVersion(gdal_version, version)>=0) {
    stop(paste("gdal v", version,
               "is required, but detected gdal v", gdal_version),
         call. = FALSE)
  }
}

