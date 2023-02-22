
# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs/mean/reference_datetime/site


# Partition: gefs/stage1/reference_datetime/site
# Schema:

library(gdalcubes)
library(parallel)
library(dplyr)
gdalcubes_options(parallel=TRUE)

devtools::load_all()
library(arrow)

Sys.setenv(VSI_CACHE="False")

# can start back to 2017-01-01
gefs_to_parquet(dates = seq(as.Date("2023-01-01"), Sys.Date()-1, by=1),
                ensemble = c(mu = "geavg", sigma = "gespr"),
                gefs_s3_dir("stage1-stats"))

gefs_to_parquet(dates = seq(as.Date("2023-01-01"), Sys.Date()-1, by=1),
                ensemble = c("gec00", paste0("gep", stringr::str_pad(1:30, 2, pad="0"))),
                gefs_s3_dir("stage1"))

