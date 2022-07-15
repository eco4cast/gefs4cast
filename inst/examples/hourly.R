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
generate_netcdf <- TRUE

base_dir <- "/home/rstudio/test_processing/noaa/gefs/stage2"

reprocess_all <- FALSE
real_time_processing <- TRUE


s3_stage1 <- arrow::s3_bucket("drivers/noaa/gefsv12/stage1", 
                              endpoint_override =  "data.ecoforecast.org",
                              anonymous=TRUE)

s3_stage2 <- arrow::s3_bucket("drivers/noaa/gefsv12", 
                              endpoint_override =  "data.ecoforecast.org",
                              anonymous=TRUE)
df <- arrow::open_dataset(s3_stage1, partitioning = c("start_date", "cycle"))

if(real_time_processing){
  dates <- as.character(seq(Sys.Date() - lubridate::days(4), Sys.Date(), by = "1 day"))
}else{
  dates <- as.character(seq(lubridate::as_date("2022-04-20"), lubridate::as_date("2022-04-22"), by = "1 day"))
}

cycles <- "00"

available_dates <- df |> 
  dplyr::filter(start_date %in% dates,
                cycle == 0,
                variable == "PRES") |> 
  distinct(start_date) |> 
  collect() %>% 
  pull(start_date)

sites <- df |> 
  dplyr::filter(start_date == available_dates[1],
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

forecast_start_times <- expand.grid(available_dates, cycles, sites) |> 
  stats::setNames(c("date", "cycle", "site_id")) |> 
  mutate(start_times = paste0(date, " ", cycle, ":00:00"),
         dir = file.path(date, cycle, site_id)) |> 
  select(site_id, date, cycle, dir)

files_present <- purrr::map_int(1:nrow(forecast_start_times), function(i, forecast_start_times){
  if(fs::dir_exists(forecast_start_times$dir[i])){
    length(fs::dir_ls(forecast_start_times$dir[i]))
  }else{
    NA
  }
},
forecast_start_times = forecast_start_times
)

forecast_start_times <- bind_cols(forecast_start_times, files_present) |> 
  rename(files_present = `...4`) |> 
  filter(is.na(files_present) | files_present < 31 | reprocess_all)

#future::plan("future::multisession", workers = 4)

purrr::walk(1:nrow(forecast_start_times),
            function(i, forecast_start_times, df, model_name, base_dir){
              
              fs::dir_create(forecast_start_times$dir[i])
              
              d1 <- df |> 
                dplyr::filter(start_date == as.character(forecast_start_times$date[i]),
                              variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
                              site_id == forecast_start_times$site_id[i],
                              cycle == as.integer(forecast_start_times$cycle[i])) |> 
                select(-c("start_date", "cycle")) |>  
                collect() |> 
                disaggregate_fluxes() |> 
                add_horizon0_time() |> 
                convert_precip2rate() |> 
                disaggregate2hourly()
              
              arrow::write_parquet(d1, sink = s3_stage2$path(file.path("parquet", forecast_start_times$dir[i],"test.parquet")))
              
              if(generate_netcdf){
                write_noaa_gefs_netcdf(d1, dir = file.path(base_dir, "ncdf", forecast_start_times$dir[i]), model_name = model_name)
                files <- fs::dir_ls(file.path(base_dir, "ncdf", forecast_start_times$dir[i]))
                purrr::map(files, function(file){
                  aws.s3::copy_object(what = file.path(base_dir, "ncdf", forecast_start_times$dir[i], file), 
                                                                     object = file.path("noaa/gefs/stage2/ncdf", forecast_start_times$dir[i], file), 
                                                                     bucket = "neon4cast-drivers") 
                  fs::file_delete(file.path(base_dir, "ncdf", forecast_start_times$dir[i], file))
                  })
              }
            },
            df = df,
            forecast_start_times= forecast_start_times,
            model_name = model_name,
            base_dir = base_dir
)







