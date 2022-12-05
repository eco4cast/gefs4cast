library(gdalcubes)
library(gefs4cast) # remotes::install_github("neon4cast/gefs4cast")
library(stringr)
library(lubridate)

## 3-hr period up to 10 days, (then every 6 hrs up to 35 day horizon)
gefs_cog("~/gefs_cog", ens_avg=TRUE,
                    max_horizon=240, date = Sys.Date() - 1) |> system.time()

# 000 file has different bands and so cannot be stacked into the collection
files <- fs::dir_ls("~/gefs_cog/", type="file", recurse = TRUE)[-1]
step <- files |> str_extract("f\\d{3}\\.tif") |> str_extract("\\d{3}") 
datetime <- Sys.Date() + hours(step) 

# pull band names out with stars, hack the timestamp off of the band name
fc <- stars::read_stars(files[1]) 
bandnames <- st_get_dimension_values(fc, 3) |> str_extract("([A-Z]+):", group = 1)

gefs_cube <- create_image_collection(files,
                                     date_time = datetime, 
                                     band_names = bandnames)

box <- sf::st_bbox(fc)
iso <-"%Y-%m-%dT%H:%M:%S"
v <- cube_view(srs = "EPSG:4326",
               extent = list(t0 = as.character(min(datetime),iso), 
                             t1 = as.character(max(datetime),iso),
                             left = box[1], right = box[3],
                             top = box[4], bottom = box[2]),
              nx = 720, ny = 361, # original resolution
               #nt = length(files), 
               dt= "PT30M",
               aggregation = "mean", resampling = "cubicspline"
)


Q <- raster_cube(gefs_cube, v)
Q |> 
   gdalcubes::select_bands("TMP") |> 
   gdalcubes::fill_time("linear") |>
   slice_time(datetime = "2022-12-05 06:30:00") |>
   plot(col = viridis)



library(stars)
library(tmap)
library(viridisLite)
data(World)
box <- st_bbox(World)


gdalcubes_options(parallel = 24)



Q |> gdalcubes::select_bands("TMP") |>
  gdalcubes::fill_time("linear") |>
  animate( col = viridisLite::viridis, fps=10, save_as = "temp.gif")

# Watch the days spin by
Q |> gdalcubes::select_bands("DSWRF") |>  
  animate( col = viridisLite::viridis, fps=10, save_as = "weather.gif")
