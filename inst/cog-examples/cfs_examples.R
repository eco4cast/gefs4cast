# ens 1
ens <- 1

reference_datetime <- as.Date("2022-03-02")

options("mc.cores"=10)
bench::bench_time({
  df <- cfs_stars_extract(ens, reference_datetime)
})

urls <- cfs_urls(ens, reference_datetime)
dates <- cfs_horizon(reference_datetime)

cfs_files <- cfs_grib_collection(ens, date, horizon=days(10))
view <- cfs_view(date)

library(gdalcubes)
gdalcubes::raster_cube(cfs_files, view) |>
    select_bands("band38") |>
    animate(col = viridisLite::viridis,
             fps=10, save_as = "temp.gif")

gdalcubes::raster_cube(cfs_files, view) |>
  gdalcubes::select_bands("band38") |>
  write_tif("~/cfs")

## not sure what is going on here...
sites <- neon_sites()
x <- gdalcubes::raster_cube(cfs_files, view) |>
  gdalcubes::select_bands("band38") |>
  gdalcubes::extract_geom(World)
x

r <- stars::read_stars("~/cfs/cube_dba3d770326762022-03-03T12:00:00.tif")
plot(r)
r <- terra::rast("~/cfs/cube_dba3d770326762022-03-03T12:00:00.tif")
plot(r)

r <- stars::read_stars(paste0("/vsicurl/",urls[[1]]))

r <- stars::read_stars(paste0("/vsicurl/", "https://noaa-cfs-pds.s3.amazonaws.com/cfs.20181031/00/6hrly_grib_01/flxf2018103100.01.2018103100.grb2"))
r[,,,36:38]

library(terra)
r <- terra::rast(paste0("/vsicurl/",urls[[1]]))
head(r)
