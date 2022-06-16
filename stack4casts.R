library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "js2.jetstream-cloud.org:8001",
                       anonymous=TRUE)

df <- arrow::open_dataset(s3)

forecast <- df |> 
  filter(start_time >= lubridate::as_datetime("2022-04-20 00:00:00"),
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
