library(gdalcubes)
library(gefs4cast) # remotes::install_github("neon4cast/gefs4cast")
library(minio) # remotes::install_github("cboettig/minio")

## Current 35 day (840 hour) forecast
gefs4cast::gefs_cog("~/gefs_cog", threads=800) |> system.time()


