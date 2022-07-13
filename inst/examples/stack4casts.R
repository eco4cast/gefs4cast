library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)

df <- arrow::open_dataset(s3)

sites <- df |> 
  dplyr::filter(start_time == lubridate::as_datetime(dates[1]),
                variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

d <- df |> dplyr::filter(variable == "PRES",
                         site_id == "BART",
                         horizon %in% c(0,3,6)) |> 
  distinct(start_time) |> collect()

sites <- df |> 
  dplyr::filter(start_time == lubridate::as_datetime(d[1]),
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

forecast <- df |> 
  filter(start_time >= lubridate::as_datetime("2020-09-25 00:00:00"),
         start_time <= max(d$start_time),
         variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
         site_id == "BART",
         horizon %in% c(0,3,6)) |> 
  collect() |> 
  disaggregate_fluxes() |> 
  add_horizon0_time() |> 
  convert_precip2rate() |> 
  filter(horizon < 6) |> 
  mutate(start_time = min(start_time)) |> 
  disaggregate2hourly() #|> 
#average_ensembles() |> 

forecast |> 
  ggplot(aes(x = time, y = predicted, group = ensemble))  +
  geom_line() +
  facet_wrap(~variable, scale = "free")
