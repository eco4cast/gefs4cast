
library(gdalcubes)
gdalcubes_options(parallel=TRUE)
devtools::load_all()

#options("mc.cores"=parallel::detectCores())


bench::bench_time({
  df <- stars_extract(1,
                      reference_datetime = Sys.Date()-32,
                      horizon = cfs_horizon(),
                      bands = cfs_band_numbers(),
                      url_builder = cfs_url)
})


# c6in.4xlarge: ~ 14 - 24 sec, 4 cores
# cirrus: 47 sec
options("mc.cores"=4L)
bench::bench_time({
  cfs_to_parquet(Sys.Date()-2,
                  sites = neon_sites())
})








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

