library(arrow)
library(dplyr)

stage2 <- function(start_date, site_id = NA){

  if(is.na(site_id)){
    bucket <- paste0("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage1/reference_datetime=",start_date)
  }else{
    bucket <-paste0("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage1/reference_datetime=",start_date,"/site_id=",site_id)

  }

  endpoint_override <- "https://sdsc.osn.xsede.org"
  s3 <- arrow::s3_bucket(paste0(bucket),
                         endpoint_override = endpoint_override,
                         anonymous = TRUE)

  site_df <- arrow::open_dataset(s3) |>
    dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::collect() |>
    mutate(reference_datetime = start_date)

  if(!is.na(site_id)){
    site_df <- site_df |> dplyr::mutate(site_id = site__id)
  }

  hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = TRUE)

  return(hourly_df)

}


stage3 <- function(start_date = NA, site_id = NA){

  bucket <- "bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/pseudo"
  endpoint_override <- "https://sdsc.osn.xsede.org"
  s3 <- arrow::s3_bucket(paste0(bucket),
                         endpoint_override = endpoint_override, anonymous = TRUE)

  df <- arrow::open_dataset(s3) |>
    filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"))
  if(!is.na(start_date)) df <- df |> filter(reference_datetime >= start_date)
  if(!is.na(site_id)) df <- df |> filter(site_id %in% site_id)

  df |>
    dplyr::collect() |>
    to_hourly(use_solar_geom = TRUE, psuedo = TRUE)
}



bench::bench_time({
  site_df <- df |> filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::collect()

  hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = TRUE)
})



stage <- "bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage1/reference_datetime=2023-03-21/site_id=HARV"
endpoint_override <- "https://sdsc.osn.xsede.org"
s3 <- arrow::s3_bucket(paste0(stage),
                       endpoint_override = endpoint_override, anonymous = TRUE)
df <- arrow::open_dataset(s3)

bench::bench_time({
  site_df <- df |> filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::collect() |>
    mutate(reference_datetime = "2023-03-21")

  hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = TRUE)

})

bench::bench_time({
  site_df <- df |> filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::collect() |>
    mutate(reference_datetime = "2023-03-21")

  hourly_df2 <- to_hourly(site_df, use_solar_geom = FALSE, psuedo = TRUE)

})


hourly_df2 |> filter(site_id %in% c("BARC","HARV"),
                     variable == "surface_downwelling_shortwave_flux_in_air",
                     lubridate::as_date(datetime) == lubridate::as_date("2023-04-10")) |>
  ggplot(aes(x = datetime, y = prediction, color = ensemble)) +
  geom_line() +
  facet_grid(variable~site_id, scale = "free")


