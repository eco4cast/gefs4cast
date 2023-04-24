
#' gefs_to_parquet
#'
#' @param dates a vector of reference_datetimes
#' @param ensemble vector of ensemble values (e.g. 'gep01', 'gep02', ...)
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
#' @param partitioning partitioning structure used in writing the parquet data
#' @export
#'
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

  # N.B. partitioning on site_id is broken in arrow 11.x
  gdalcubes_cloud_config()
  assert_gdal_version("3.4.0")
  family <- "ensemble"
  datetime <- "dummy"
  if(any(grepl("gespr", ensemble))) family <- "spread"

  lapply(dates, function(reference_datetime) {
    message(reference_datetime)
    tryCatch({
    df0 <- megacube_extract(reference_datetime,
                        ensemble = ensemble,
                        horizon = "000",
                        sites = sites,
                        bands = gefs_bands(TRUE),
                        all_bands = gefs_all_bands(TRUE),
                        url_builder = url_builder,
                        cycles = cycle)

    df1 <- megacube_extract(reference_datetime,
                        ensemble = ensemble,
                        horizon = horizon,
                        sites = sites,
                        bands = bands,
                        all_bands = all_bands,
                        url_builder = url_builder,
                        cycles = cycle)

    dplyr::bind_rows(df1, df0)  |>
      dplyr::mutate(reference_datetime =
                      lubridate::as_date(reference_datetime),
                    horizon =
                      lubridate::as_datetime(datetime) -
                      lubridate::as_datetime(reference_datetime)) |>
      dplyr::mutate(family = family) |>
      arrow::write_dataset(path, partitioning=partitioning)
    },
    error = function(e) warning(paste("date", reference_datetime, "failed with:\n", e),
                                call.=FALSE),
    finally=NULL)
    })

  invisible(dates)
}

#' gefs_s3_dir
#'
#' Helper function that returns a path to the EFI S3 directory
#' @param product product code, e.g. stage1
#' @param path path inside bucket
#' @param endpoint endpoint url
#' @param gefs_version gefs-version (used in path)
#' @param bucket bucket name
#' @return s3 bucket object (an arrow S3 SubTreeFileSystem object)
#' @export
#' @examples
#' \donttest{
#' gefs_s3_dir()
#' }
gefs_s3_dir <- function(product = "stage1",
                        path = "neon4cast-drivers/noaa/",
                        gefs_version = Sys.getenv("GEFS_VERSION", "v12"),
                        endpoint = "https://sdsc.osn.xsede.org",
                        bucket = "bio230014-bucket01")
{
  if(gefs_version == "v11.1") gefs_version <- "v11"

  bucket_path = fs::path(bucket, path, paste0("gefs-", gefs_version), product)
  s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                   access_key = Sys.getenv("OSN_KEY"),
                                   secret_key = Sys.getenv("OSN_SECRET"))
  s3_dir <- arrow::SubTreeFileSystem$create(bucket_path, s3)
  s3_dir
}


#' gefs metadata
#'
#' table of data bands accessed from GEFS forecasts,
#' along with descriptions
#' @export
gefs_metadata <- function() {
  system.file("extdata/gefs-selected-bands.csv",
              package="gefs4cast") |>
    readr::read_csv(show_col_types = FALSE)
}
#' mapping of gefs_bands to variable names
#' @param zero_horizon GEFS zero-horizon data uses different band numbering,
#' so this must be set to TRUE if zero-horizon data is desired.
#' @param gefs_version GEFS version: v11 covers reference_datetimes from
#' Jan 1, 2017 to Sept 24, 2020. v12 covers all dates following that.
#' (earlier versions are not available on AWS).
#' Note that gefs v11 has only 20 ensemble members, 16 day horizons, and
#' all at 6 hour intervals.  gefs v11 also has fewer bands.
#' export
gefs_bands <- function(zero_horizon = FALSE,
                       gefs_version = Sys.getenv("GEFS_VERSION", "v12")) {
  meta <- gefs_metadata()

  # ACTUALLY, v11 adopts v12 band numbering on 2018-07-19, not 2018-07-27
  if(gefs_version == "v11.1") gefs_version <- "v12"

  if(zero_horizon && gefs_version == "v12"){
    meta <- meta[!is.na(meta$horiz0_number), ]
    bands <- paste0("band", meta$horiz0_number)
    names(bands) <- meta$Parameter
  } else if (!zero_horizon && gefs_version == "v12" ) {
    bands <- paste0("band", meta$Number)
    names(bands) <- meta$Parameter
  } else if (!zero_horizon && gefs_version == "v11" ) {
    meta <- meta[!is.na(meta$v11_number), ]
    bands <- paste0("band", meta$v11_number)
    names(bands) <- meta$Parameter
  } else if (zero_horizon && gefs_version == "v11" ) {
    meta <- meta[!is.na(meta$v11_horiz0), ]
    bands <- paste0("band", meta$v11_horiz0)
    names(bands) <- meta$Parameter
  }
  bands
}

gefs_all_bands <- function(zero_horizon = FALSE,
                           gefs_version = Sys.getenv("GEFS_VERSION", "v12")){

  # ACTUALLY, v11 adopts v12 band numbering on 2018-07-19, not 2018-07-27
  if(gefs_version == "v11.1") gefs_version <- "v12"

  if(zero_horizon){
    out <- switch(gefs_version,
                  "v12" = paste0("band", 1:71),
                  "v11" = paste0("band", 1:69))
  } else {
    out <- switch(gefs_version,
                  "v12" = paste0("band", 1:85),
                  "v11" = paste0("band", 1:83))
  }
out
}


# https://www.nco.ncep.noaa.gov/pmb/products/gens/
#' gefs_urls
#'
#' @inheritParams gefs_to_parquet
#' @inheritParams gefs_bands
#' @param reference_datetime date forecast is produced
#' @param ens ensemble member for which URLs should be generated
#' @param series data series (used only by gefs_v12)
#' @param base NOAA GEFS AWS Bucket
#' @export
gefs_urls <- function(ens = "geavg",
                      reference_datetime = Sys.Date(),
                      horizon = gefs_horizon(),
                      cycle = "00",
                      series = "atmos",
                      gefs_version = Sys.getenv("GEFS_VERSION", "v12"),
                      base = "https://noaa-gefs-pds.s3.amazonaws.com") {
  reference_datetime <- lubridate::as_date(reference_datetime)
  date_time <- reference_datetime + lubridate::hours(horizon)

  # v11 goes from 2017-01-01 to 2020-09-22.  BUT
  # on 2018-07-27, file name and url structure shift slightly

  gribs <- switch(gefs_version,
                  "v12" = paste0(
                                base,
                                "/gefs.",format(reference_datetime, "%Y%m%d"),
                                "/", cycle, "/", series, "/pgrb2ap5/",
                                ens, ".t", cycle, "z.", "pgrb2a.0p50.",
                                "f", horizon),
# https://noaa-gefs-pds.s3.amazonaws.com/gefs.20170101/00/gec00.t00z.pgrb2af006
                  "v11" = paste0(base,  "/gefs.",
                                 format(reference_datetime, "%Y%m%d"),
                                 "/", cycle, "/", ens, ".t", cycle, "z.",
                                 "pgrb2a", "f", horizon),
                  "v11.1" = paste0(base, "/gefs.",
                                   format(reference_datetime, "%Y%m%d"),
                                   "/", cycle, "/pgrb2a/",
                                   ens, ".t", cycle, "z.", "pgrb2a",
                                   "f", horizon)
      )
  paste0("/vsicurl/", gribs)
}


#' gefs_horizon
#' @inheritParams gefs_bands
#' @param ... additional parameters (not used, for cross-compatibility only)
#' @return list of horizon values (for cycle 00, gepNN forecasts)
#' @export
gefs_horizon <- function(gefs_version = Sys.getenv("GEFS_VERSION", "v12"),
                         ...) {
  switch(gefs_version,
         "v12" = c(stringr::str_pad(seq(3,240,by=3), 3, pad="0"),
                   stringr::str_pad(seq(246,840,by=6), 3, pad="0")),
         "v11" = stringr::str_pad(seq(6,384, by=6), 3, pad="0"),
         "v11.1" = stringr::str_pad(seq(6,384, by=6), 2, pad="0")
  )
}


#' gefs ensemble list
#' @inheritParams gefs_bands
#' @return Generates the strings for the 30 perturbed ensembles and control
#' If only mean and spread are needed, manually pass
#' `c(mean = "geavg", spr = "gespr")`
#' @export
gefs_ensemble <- function(gefs_version = Sys.getenv("GEFS_VERSION", "v12")) {
  switch(gefs_version,
         "v12" = c("gec00", paste0("gep", stringr::str_pad(1:30, 2, pad="0"))),
         "v11" = c("gec00", paste0("gep", stringr::str_pad(1:20, 2, pad="0"))),
         "v11.1" = c("gec00", paste0("gep", stringr::str_pad(1:20, 2, pad="0")))
  )
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

