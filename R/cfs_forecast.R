

#band 5: tmp (surface) - not used but is automatically included
#band ?: longwave (surface)
#band ?: shortwave (surface)
#band 31: rain (PRATE)
#band 36.1: uwind (10 m)
#band 37  ("36.2"): v wind (10 m)
#band 38: Tmp (2m)
#band 39: specific humidity (2 m) SPFH
#band 40: pressure (surface)
# soilW




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
                        horizon=days(273)) {

  start <- lubridate::as_datetime(reference_datetime)
  n = 1 + ( horizon / hours(6) )
  out <- seq(start, start + horizon, length.out=n)
  stopifnot(out[2] - out[1] == as.difftime(6, units="hours"))
  out[-1] # first horizon has different bands
}


cfs_stars_extract <- function(ens,
                              reference_datetime = Sys.Date()-1,
                              horizon = days(273),
                              cycle = "00",
                              interval="6hrly",
                              sites = neon_sites(),
                              ...) {
  reference_datetime <- lubridate::as_date(reference_datetime)

  date_times <- cfs_horizon(reference_datetime, horizon)

  sites <- sites |>
    sf::st_transform(crs = sf::st_crs(grib_wkt())) |>
    dplyr::select("site_id", "geometry")

  bands <- c(31, 36:40)

  parallel::mclapply(date_times, function(datetime) {
    cfs_url(datetime, ens, reference_datetime, cycle, interval) |>
    stars::read_stars() |>
    select_bands_(bands) |>
    extract_sites_(sites) |>
    dplyr::mutate(parameter = ens,
                  datetime = datetime,
                  reference_datetime = reference_datetime,
                  family="ensemble")
  }, mc.cores = getOption("mc.cores", 1L)) |>
    purrr::list_rbind()

}


select_bands_ <- function(r, bands){
  r[,,,unname(bands)]
}

extract_sites_ <- function(r, sites) {

  if (!identical(sf::st_crs(r), sf::st_crs(sites))) {
    sites <- sf::st_transform(sites, st_crs(r))
  }

  y <- stars::st_extract(r, sf::st_coordinates(sites))

  variables <- stars::st_get_dimension_values(r,3)
  variables <- gsub("(\\w+):.*", "\\1", variables)
  colnames(y) <- variables

  vctrs::vec_cbind(y, sites) |>
    tibble::as_tibble() |>
    tidyr::pivot_longer(variables,
                        names_to="variable",
                        values_to="prediction")
}










cfs_urls <- function(ens = 1,
                     reference_datetime=Sys.Date()-2,
                     horizon = days(273),
                     cycle = "00",
                     interval = "6hrly") {
  cfs_horizon(reference_datetime,horizon) |>
    purrr::map_chr(cfs_url, ens, reference_datetime, cycle, interval)
}


cfs_grib_collection <- function(ens,
                                date = Sys.Date()-1,
                                horizon = days(273),
                                cycle = "00",
                                interval="6hrly",
                                ...) {
  reference_datetime <- lubridate::as_date(date)
  date_time <- cfs_horizon(reference_datetime, horizon)
  urls <- cfs_urls(ens, reference_datetime, horizon, cycle, interval)
  gribs <- paste0("/vsicurl/", urls)
  gdalcubes::create_image_collection(gribs, date_time = date_time, ...)
}

cfs_view <- function ( t0 = Sys.Date()-1,
                       t1 = as.Date(t0) + 90L, #- lubridate::seconds(1),
                       box = cfs_bbox(),
                       dx = 0.5,
                       dy = 0.5,
                       dt = "PT6H",
                       crs = grib_wkt(),
                       ...
){
  t0 <- lubridate::as_datetime(t0)
  t1 <- lubridate::as_datetime(t1)
  gdalcubes::cube_view(srs = crs,
                       extent = list(left = box[1], right = box[3],
                                     top = box[4], bottom = box[2],
                                     t0= format(t0, "%Y-%m-%dT%H:%M:%SZ"),
                                     t1 = format(t1, "%Y-%m-%dT%H:%M:%SZ")),
                       dx = dx, dy = dy, dt = dt, ...)

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
