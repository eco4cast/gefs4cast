# ens 1
ens <- 1
date <- as.Date("2022-02-01")

#urls <- cfs_urls(1, )
cfs_files <- cfs_grib_collection(ens, date)
view <- cfs_view(date)

library(gdalcubes)
gdalcubes::raster_cube(cfs_files, view) |>
    select_bands("band37") |>
    animate(col = viridisLite::viridis,
             fps=10, save_as = "temp.gif")


sites <- neon_sites()
gdalcubes::raster_cube(cfs_files, view) |>
  gdalcubes::select_bands("band37") |>
  gdalcubes::extract_geom(sites)

r <- stars::read_stars(paste0("/vsicurl/",urls[[1]]))
r[,,,5]|> plot()

library(terra)
r <- terra::rast(paste0("/vsicurl/",urls[[1]]), names="band7")
head(r)
