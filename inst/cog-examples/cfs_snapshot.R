
devtools::load_all()
options("mc.cores"=parallel::detectCores())

ens <- 1
reference_datetime = Sys.Date()-2

s3 <- cfs_s3_dir(product=glue::glue("6hrly"))

dates <- rev(seq(as.Date("2023-01-01"), Sys.Date()-1, by=1))

lapply(dates, function(reference_datetime){
  print(reference_datetime)
  lapply(1:4, function(ens){
    cfs_stars_extract(ens, reference_datetime) |>
      dplyr::select(-geometry) |>
      arrow::write_dataset(s3, partitioning=c("reference_datetime", "site_id"))
  })
})


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
