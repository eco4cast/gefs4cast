

#band 1: tmp (surface) - not used but is automatically included
#band 2: longwave (surface)
#band 3: shortwave (surface)
#band 4: rain (PRATE)
#band 5: uwind (10 m)
#band 6: v wind (10 m)
#band 7: Tmp (2m) used
#band 8: specific humidity (2 m)
#band 9: pressure (surface)

cfs_urls <- function(reference_datetime=Sys.Date()-2,
                     horizon_days = 90,
                     cycle = "00",
                     interval = "6hrly",
                     ens = 1) {
  base = "https://noaa-cfs-pds.s3.amazonaws.com"
  purrr::map_chr(cfs_horizon(reference_datetime,horizon_days),
                 function(datetime) {
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
  })
}

cfs_horizon <- function(reference_datetime = Sys.Date()-1, horizon_days=90L) {
  seq(lubridate::as_datetime(reference_datetime),
      lubridate::as_datetime(reference_datetime) +
        lubridate::days(horizon_days),
      length.out=1 + (4*horizon_days))
}

cfs_grib_collection <- function(ens,
                                date = Sys.Date()-1,
                                horizon = 90,
                                cycle = "00",
                                ...) {
  reference_datetime <- lubridate::as_date(date)
  date_time <- cfs_datetime(reference_datetime, horizon)
  urls <- cfs_urls(reference_datetime, horizon, cycle, ens)
  gribs <- paste0("/vsicurl/", urls)
  gdalcubes::create_image_collection(gribs, date_time = date_time, ...)
}

cfs_view <- function ( t0 = Sys.Date()-1,
                       t1 = as.Date(t0) + 90L - lubridate::seconds(1),
                       box = gefs_bbox(),
                       dx = 0.5,
                       dy = 0.5,
                       dt = "PT6H",
                       crs = "EPSG:4326",
                       ...
)

"https://noaa-cfs-pds.s3.amazonaws.com/cfs.20181031/00/6hrly_grib_01/flxf2018103100.01.2018103100.grb2"

stars::read_stars(urls[[1]]) |> sf::st_bbox()
terra::rast(urls[[1]])
