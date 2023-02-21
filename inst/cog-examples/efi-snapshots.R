
# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs/mean/reference_datetime/site


# Partition: gefs/stage1/reference_datetime/site
# Schema:

library(gefs4cast)
library(gdalcubes)
library(parallel)
library(dplyr)
gdalcubes_options(parallel=TRUE)

devtools::load_all()
library(arrow)
endpoint <- "https://sdsc.osn.xsede.org"
bucket <- "bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage1-stats/"
s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint,
                                 access_key = Sys.getenv("OSN_KEY"),
                                 secret_key = Sys.getenv("OSN_SECRET"))
s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)

sf_sites <- neon_sites()
#ensemble <-  paste0("gep", stringr::str_pad(1:30, 2, pad="0"))
ensemble <- c(mu = "geavg", sigma = "gespr") # mean and spread

cores <- length(ensemble)
dates <- seq(as.Date("2022-01-01"), Sys.Date()-1, by=1)
bench::bench_time({
  for(date in dates) {
    lapply(ensemble, grib_extract, date = date, sites = sf_sites) >
    efi_format_cubeextract(sf_sites) |>
    dplyr::mutate(family = "normal") |>
    arrow::write_dataset(s3_dir,
                       partitioning = c("reference_datetime", "site_id"))
  }
})

