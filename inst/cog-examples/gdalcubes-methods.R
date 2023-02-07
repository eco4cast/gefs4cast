
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
date <- as.Date("2022-11-01")


neon_sites <- function() {
  sites <- readr::read_csv(paste0("https://github.com/eco4cast/",
                                  "neon4cast-noaa-download/",
                                "raw/master/noaa_download_site_list.csv"))
  sf_sites <- sf::st_as_sf(sites,coords=c("longitude", "latitude"),
                           crs = 4326) |>
  tibble::rowid_to_column("FID")
}


gefs_bbox <- function(){
  grib <- paste0("/vsicurl/",
                 "https://noaa-gefs-pds.s3.amazonaws.com/gefs.",
                 format(date, "%Y%m%d"), "/00/atmos/pgrb2ap5/",
                 "gep01", ".t00z.pgrb2a.0p50.f003")
  box <- stars::read_stars(grib) |> sf::st_bbox()
  box
}


gefs_cube_view <- function (t0 = Sys.Date(),
                            t1 = t0 + 35L,
                            box = gefs_bbox(),
                            dx = 0.5, dy = 0.5, dt = "PT3H",
                            crs = "EPSG:4326",
                            ...
                            ) {
 cube_view(srs = crs,
           extent = list(left = box[1], right = box[3],
                         top = box[4], bottom = box[2],
                         t0= as.character(t0),
                         t1=as.character(t1)),
           dx = dx, dy = dy, dt = dt, ...)

}


grib_datacube <- function(ens, view, ...) {

  date <- lubridate::as_date(view$time$t0)
  horizon <- c(stringr::str_pad(seq(3,240,by=3), 3, pad="0"),
               stringr::str_pad(seq(246,840,by=6), 3, pad="0"))
  date_time = date + lubridate::hours(horizon)

  # filter horizon to max time, if necessary
  horizon <- horizon[date_time < view$time$t1]
  date_time <- date_time[date_time < view$time$t1]

  gribs <- paste0("/vsicurl/",
                  "https://noaa-gefs-pds.s3.amazonaws.com/gefs.",
                  format(date, "%Y%m%d"), "/00/atmos/pgrb2ap5/",
                  ens, ".t00z.pgrb2a.0p50.f", horizon)

  gdalcubes::create_image_collection(gribs, date_time = date_time, ...)

}


grib_to_tif <- function(ens,
                        view = gefs_cube_view(),
                        dir = NULL,
                        creation_options=list(COMPRESS="zstd")) {

  if(is.null(dir)) {
    date <- format(lubridate::as_date(view$time$t0), "%Y%m%d")
    dir <- fs::dir_create(paste0("gefs.", date))
  }

  datacube <- grib_datacube(ens, view)
  raster_cube(datacube, view) |>
    select_bands(paste0("band", c(57, 63, 64, 67, 68, 69, 78, 79))) |>
    gdalcubes::write_tif(dir,
                         prefix=paste0(ens, ".t00z.pgrb2a.0p50.f"),
                         COG=TRUE,
                         creation_options = creation_options)
}


#ensemble <- c("geavg","gec00", ensemble)
bench::bench_time({
  view <- gefs_cube_view(date)
  ensemble <-  paste0("gep", stringr::str_pad(1:30, 2, pad="0"))
  lapply(ensemble, grib_to_tif, view= view)
})



sf_sites <- neon_sites()


# remotes::install_github("cboettig/minio")
library(minio)
install_mc()
mc_alias_set("osn",
             endpoint = "sdsc.osn.xsede.org",
             access_key=Sys.getenv("OSN_KEY"),
             secret_key =Sys.getenv("OSN_SECRET"))

mc(glue::glue("cp -r {dir} osn/bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/COG"))
mc(glue::glue("cp mirror osn/bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/COG nvme/noaa/cog"))

fs::dir_delete(dir)


## GRIBS

grib_extract <- function(ens) {
  gribs <- paste0("/vsicurl/",
                  "https://noaa-gefs-pds.s3.amazonaws.com/gefs.",
                  format(date, "%Y%m%d"), "/00/atmos/pgrb2ap5/",
                  ens, ".t00z.pgrb2a.0p50.f", horizon)

  cube <- gdalcubes::create_image_collection(gribs, date_time = date_time)

  df <- raster_cube(cube, v) |>
    select_bands(paste0("band", c(57, 63, 64, 67, 68, 69, 78, 79))) |>
    extract_geom(sf_sites)

  df
}

bench::bench_time({ # 1.39 hrs
  dfs <- lapply(ensemble, grib_extract)
})

df <- purrr::list_rbind(dfs, names_to = "ensemble") |> tibble::as_tibble()


## OSN COG

library(gdalcubes)
library(arrow)
s3 <- arrow::S3FileSystem$create(endpoint_override = "https://sdsc.osn.xsede.org", anonymous=TRUE)
s3_dir <- arrow::SubTreeFileSystem$create("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/COG/", s3)
tifs <- s3_dir$ls(recursive=TRUE)
osn_tifs <- paste0("/vsicurl/",
                   "https://sdsc.osn.xsede.org",
                   "/bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/COG/",
                   tifs)
cog_extract <- function(ens) {
  tifs <- osn_tifs[grepl(ens, osn_tifs)]
  datetimes <- lubridate::as_datetime( stringr::str_extract(tifs, "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}") )
  band_names =  c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")
  cube <- gdalcubes::create_image_collection(tifs, date_time = datetimes, band_names = band_names)
  df <- raster_cube(cube, v) |>  extract_geom(sf_sites)
  df
}

bench::bench_time({ # 11 min
  dfs <- lapply(ensemble, cog_extract)  |>
    purrr::list_rbind(names_to="ensemble") |>
    tibble::as_tibble()
})




## NVME COG 2min
s3 <- arrow::S3FileSystem$create(endpoint_override = "minio.carlboettiger.info", anonymous=TRUE)
s3_dir <- arrow::SubTreeFileSystem$create("noaa/cog", s3)
tifs <- s3_dir$ls(recursive=TRUE)
nvme_tifs <- paste0("/vsicurl/",
                    "https://minio.carlboettiger.info",
                    "/noaa/cog/",
                    tifs)

cog_extract <- function(ens) {
  tifs <- nvme_tifs[grepl(ens, nvme_tifs)]
  datetimes <- lubridate::as_datetime( stringr::str_extract(tifs, "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}") )
  band_names =  c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF")
  cube <- gdalcubes::create_image_collection(tifs, date_time = datetimes, band_names = band_names)
  df <- raster_cube(cube, v) |>  extract_geom(sf_sites)
  df
}

bench::bench_time({ # ~ 2 min
  dfs <- lapply(ensemble, cog_extract) |>
    purrr::list_rbind(names_to="ensemble") |>
    tibble::as_tibble()
})

#https://sdsc.osn.xsede.org/bio230014-bucket01

library(dplyr)
bench::bench_time({

  s3 <- arrow::S3FileSystem$create(endpoint_override = "https://data.ecoforecast.org",
    #endpoint_override = "https://sdsc.osn.xsede.org",
                                   anonymous=TRUE)
  s3_dir <- arrow::SubTreeFileSystem$create("neon4cast-drivers/noaa/gefs-v12/stage1/0", s3)
  ds <- arrow::open_dataset(s3_dir,  partitioning = c("reference_date"))
  df <- ds |> dplyr::filter(reference_date == "2022-11-01") |> dplyr::collect()
})


#band_names =  c("PRES",  "TMP"  , "RH"  ,  "UGRD" , "VGRD" , "APCP" , "DSWRF", "DLWRF")
#b 57 -b 63 -b 64 -b 67 -b 68 -b 69 -b 78 -b 79


