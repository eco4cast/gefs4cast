library(gdalcubes)
library(gefs4cast) # remotes::install_github("neon4cast/gefs4cast")
library(minio) # remotes::install_github("cboettig/minio")
library(stringr)
library(lubridate)
## Current 35 day (840 hour) forecast
gefs4cast::gefs_cog("~/gefs_cog", threads=800, ens_avg=TRUE) |> system.time()

# 000 file has different bands and so cannot be stacked into the collection
files <- fs::dir_ls("~/gefs_cog/", type="file", recurse = TRUE)[-1]
step <- files |> str_extract("f\\d{3}\\.tif") |> str_extract("\\d{3}") 

datetime <- Sys.Date() + hours(step)
gefs.collection <- create_image_collection(files, date_time = datetime)
