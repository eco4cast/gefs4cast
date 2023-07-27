sites <- c("ABBY","HARV")
date <- "2020-10-01"

site_list <- readr::read_csv(paste0("https://github.com/eco4cast/",
                                    "neon4cast-noaa-download/",
                                    "raw/master/noaa_download_site_list.csv"),
                             show_col_types = FALSE) |>
  dplyr::filter(site_id %in% sites)

site_1hourly <- to_hourly(i = 1, site_list = site_list, date = date, use_solar_geom = TRUE)
ggplot(site_1hourly, aes(x = datetime, y = prediction, group = ensemble)) + geom_line() + facet_wrap(~variable, scale = "free")

# Multiple dates Stage 2

for(i in 1:date_list){

site_1hourly <- map_dfr(1:nrow(site_list), to_hourly, site_list, date)

s3 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage2", endpoint_override = "https://sdsc.osn.xsede.org")

arrow::write_dataset(site_1hourly, path = s3, partitioning = c("reference_datetime, site_id"))

}

# Multiple dates Stage 3

for(i in 1:date_list){

  site_1hourly <- map_dfr(1:nrow(site_list), to_hourly, site_list, date, stage1 = "bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/pseudo")

  s3 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage3", endpoint_override = "https://sdsc.osn.xsede.org")

  arrow::write_dataset(site_1hourly, path = s3, partitioning = c("reference_datetime, site_id"))

}

