library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)

df <- arrow::open_dataset(s3, partitioning = c("start_date", "offset"))

d <- df |> dplyr::filter(variable == "PRES",
                         site_id == "BART",
                         horizon %in% c(0,3)) |> 
  distinct(start_date) |> collect()

sites <- df |> 
  dplyr::filter(start_date == d$start_date[1],
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

forecast <- df |> 
  filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
         site_id == "BART",
         horizon %in% c(0,3)) |> 
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
