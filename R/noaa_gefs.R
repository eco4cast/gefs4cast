
#' noaa_gefs
#'
#' Compatibility wrapper for noaa gefs stage1 access
#' @param date reference_datetime
#' @param cycle start hour of reference datetime
#' @param s3 s3 bucket or local path
#' @export
noaa_gefs <- function (date = Sys.Date(),
                       cycle = "00", ...,
                       s3 = arrow::s3_bucket("drivers",
                                             endpoint_override = "data.ecoforecast.org")
) {
  gefs_to_parquet(date, cycle = cycle, path = s3)
}
