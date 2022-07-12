library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
model_name <- "NOAAGEFS_1hr"
base_dir <- "~/Downloads"
reprocess_all <- FALSE

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)



df <- arrow::open_dataset(s3)

dates <- seq(lubridate::as_date("2022-04-20"), lubridate::as_date("2022-04-22"), by = "1 day")
cycle <- c("00:00:00")




sites <- df |> 
  dplyr::filter(start_time == lubridate::as_datetime(forecast_start_times[1]),
                variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

forecast_start_times <- expand.grid(dates, cycle, sites) |> 
  stats::setNames(c("date", "cycle", "site_id")) |> 
  mutate(start_times = paste(date, cycle),
         cycle = stringr::str_pad(lubridate::hour(lubridate::as_datetime(start_times)),2, "left", 0),
         dir = file.path(base_dir, model_name, site_id, date, cycle)) |> 
  select(site_id, start_times, dir)


files_present <- purrr::map_int(1:nrow(forecast_start_times), function(i, forecast_start_times){
  if(fs::dir_exists(forecast_start_times$dir[i])){
    length(fs::dir_ls(forecast_start_times$dir[i]))
  }else{
    NA
  }
},
forecast_start_times = forecast_start_times)

forecast_start_times <- bind_cols(forecast_start_times, files_present) |> 
  rename(files_present = `...4`) |> 
  filter(is.na(files_present) | files_present < 31 | reprocess_all)


future::plan("future::multisession", workers = 4)

purrr::walk(1:nrow(forecast_start_times),
                   function(i, forecast_start_times, df, model_name, base_dir){
                     df |> 
                       dplyr::filter(start_time == lubridate::as_datetime(forecast_start_times$start_times[i]),
                                     variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
                                     site_id == forecast_start_times$site_id[i]) |> 
                       collect() |> 
                       disaggregate_fluxes() |> 
                       add_horizon0_time() |> 
                       convert_precip2rate() |> 
                       disaggregate2hourly() |> 
                       write_noaa_gefs_netcdf(dir = base_dir, model_name = model_name, add_directory = TRUE)
                   },
                   df = df,
                   forecast_start_times= forecast_start_times,
                   model_name = model_name,
                   base_dir = base_dir
                   
)

forecast |> 
  ggplot(aes(x = time, y = predicted, group = ensemble))  +
  geom_line() +
  facet_wrap(~variable, scale = "free")




