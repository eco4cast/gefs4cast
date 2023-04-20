#' gdalcubes cloud config
#'
#' set the recommended environmental variables for cloud-based access
#' @export
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
