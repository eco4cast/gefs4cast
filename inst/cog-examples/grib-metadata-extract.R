url0 <- "https://noaa-gefs-pds.s3.amazonaws.com/gefs.20170101/00/gec00.t00z.pgrb2af000" # bands 1..69
url <- "https://noaa-gefs-pds.s3.amazonaws.com/gefs.20170101/00/gec00.t00z.pgrb2af006" # bands 1..69

library(terra)
zero <- terra::rast(paste0("/vsicurl/", url0))
nonzero <-  terra::rast(paste0("/vsicurl/", url))

descriptions_0 <- names(zero)
descriptions <- names(nonzero)



v11 <- arrow::open_dataset("v11")

## not so good
library(stars)
zero <- stars::read_stars(paste0("/vsicurl/", url0))
nonzero <-  stars::read_stars(paste0("/vsicurl/", url))

descriptions_0 <- st_get_dimension_values(zero, 3)
descriptions <- st_get_dimension_values(nonzero, 3)
