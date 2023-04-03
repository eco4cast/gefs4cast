
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

#options("mc.cores"=parallel::detectCores())

# c6in.4xlarge:
# cirrus:
options("mc.cores"=4L)
bench::bench_time({
  cfs_to_parquet(Sys.Date()-4)
})


bench::bench_time({
  df <- stars_extract("1",
                      reference_datetime = Sys.Date()-32,
                      horizon = cfs_horizon(),
                      bands = cfs_band_numbers(),
                      url_builder = cfs_urls)
})




dfs <- grib_extract("1",
                    reference_datetime = Sys.Date()-4,
                    bands = cfs_bands(),
                    sites = neon_sites() |> sf::st_shift_longitude(),
                    horizon = cfs_horizon(),
                    all_bands = cfs_all_bands(),
                    url_builder = cfs_urls,
                    cycle = "00")


reference_datetime <- Sys.Date()-1
ens <- 1
bands = cfs_bands()
sites = neon_sites() |> sf::st_shift_longitude()
horizon = cfs_horizon()
all_bands = cfs_all_bands()
url_builder = cfs_urls
cycle = "00"

gdalcubes_cloud_config()
reference_datetime <- lubridate::as_date(reference_datetime)
date_time <- reference_datetime + lubridate::hours(horizon)

cfs_urls(ens, reference_datetime, horizon, cycle, ...) |>
  gdalcubes::stack_cube(datetime_values = date_time,
                        band_names = all_bands) |>
  gdalcubes::select_bands(bands) |>
  gdalcubes::extract_geom(sites)



library(arrow)
reference_datetime <- Sys.Date()-1
ens <- 1
ref_datetime <- format(reference_datetime, "%Y%m%d")
s3 <- s3_bucket(glue::glue("noaa-cfs-pds/cfs.{ref_datetime}/00/6hrly_grib_0{ens}/"), anonymous=TRUE)
all_gribs <- s3$ls()
flxf <- all_gribs[ grepl("flxf\\d{10}.*\\.grb2$", all_gribs) ]
horizons <- gsub(paste0("flxf(\\d{10})\\.0", ens, "\\.(\\d{10})\\.grb2"), "\\1", flxf) |> lubridate::as_datetime(format="%Y%m%d%H")

diff <- horizons - lubridate::as_datetime(reference_datetime)
units(diff) <- "days"
diff

b <- reference_datetime + horizon

## examine

library(tidyverse)
library(arrow)
product = "6hrly/cycle=00"
path = "neon4cast-drivers/noaa/cfs"
endpoint = "https://sdsc.osn.xsede.org"
bucket = glue::glue("bio230014-bucket01/{path}/{product}")

s3 <- arrow::S3FileSystem$create(endpoint_override = endpoint, anonymous = TRUE)
s3_dir <- arrow::SubTreeFileSystem$create(bucket, s3)
cfs <- arrow::open_dataset(s3_dir)
cfs |> head() |> collect()

# use a nearby range of ref-datetimes as extra ensmble members
cfs |>
  filter(site_id == "BARR", variable == "TMP",
         reference_datetime %in% c("2023-03-26", "2023-03-27", "2023-03-28")) |>
  mutate(ensemble = paste(parameter, reference_datetime, "-")) |>
  collect() |>
  group_by(datetime) |> mutate(mean = mean(prediction)) |>
  ggplot(aes(datetime, prediction, col=ensemble)) + geom_line() +
  geom_line(aes(datetime, mean), col="darkred") +
  ggtitle("6hrly temp in Utqiaġvik, Alaska") + scale_color_viridis_d()

