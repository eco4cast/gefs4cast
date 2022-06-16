library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "js2.jetstream-cloud.org:8001",
                       anonymous=TRUE)

df <- arrow::open_dataset(s3)

  forecast <- df |> 
    filter(start_time == lubridate::as_datetime("2022-04-20 00:00:00"),
           variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
           site_id == "BART") |> 
    collect() |> 
    disaggregate_fluxes() |> 
    add_horizon0_time() |> 
    convert_precip2rate() |> 
    disaggregate2hourly() #|> 
    #average_ensembles() |> 
    #write_noaa_gefs_netcdf(dir = "~/Downloads", model_name = "NOAAGEFS_1hr")



  forecast |> 
    ggplot(aes(x = time, y = predicted, color = factor(ensemble)))  +
    geom_line() +
    facet_wrap(~variable, scale = "free")

  


