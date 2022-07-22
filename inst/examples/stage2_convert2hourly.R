library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)
readRenviron("~/.Renviron")

source(system.file("inst","examples","temporal_disaggregation.R", package = "gefs4cast"))

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
model_name <- "NOAAGEFS_1hr"
generate_netcdf <- TRUE
write_s3 <- TRUE
reprocess_all <- TRUE
real_time_processing <- FALSE
parquet_file_basename <- "neon"
base_dir <- "/home/rstudio/test_processing/noaa/gefs-v12"

s3_stage1 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage1", 
                              endpoint_override =  "data.ecoforecast.org",
                              anonymous=TRUE)
if(write_s3){
  s3_stage2 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12", 
                                endpoint_override =  "data.ecoforecast.org")
  s3_stage2$CreateDir("stage2/parquet")
  s3_stage2_parquet <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage2/parquet", 
                                        endpoint_override =  "data.ecoforecast.org")
  if(generate_netcdf){
    s3_stage2_ncdf_local <- file.path(base_dir,"stage2", "ncdf")
    s3_stage2_ncdf <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage2/ncdf", 
                                       endpoint_override =  "data.ecoforecast.org")
    
    fs::dir_create(s3_stage2_ncdf_local)
  }
}else{
  s3_stage2_parquet <- SubTreeFileSystem$create(file.path(base_dir,"stage2", "parquet"))
  s3_stage2_parquet$CreateDir("stage2/parquet")
  if(generate_netcdf){
    s3_stage2_ncdf_local <- file.path(base_dir,"stage2", "ncdf")
    s3_stage2_ncdf <- s3_stage2_ncdf_local
    fs::dir_create(s3_stage2_ncdf)
  }
}

df <- arrow::open_dataset(s3_stage1, partitioning = c("start_date", "cycle"))

if(real_time_processing){
  dates <- as.character(seq(Sys.Date() - lubridate::days(4), Sys.Date(), by = "1 day"))
}else{
  dates <- as.character(seq(lubridate::as_date("2022-04-17"), lubridate::as_date("2022-04-19"), by = "1 day"))
}

cycles <- "00"

available_dates <- df |> 
  dplyr::filter(start_date %in% dates,
                cycle == 0,
                variable == "PRES") |> 
  distinct(start_date) |> 
  collect() %>% 
  pull(start_date)

forecast_start_times <- expand.grid(available_dates, cycles) |> 
  stats::setNames(c("date", "cycle")) |> 
  mutate(start_times = paste0(date, " ", cycle, ":00:00"),
         dir = file.path(date, cycle)) |> 
  select(date, cycle, dir)

files_present <- purrr::map_int(1:nrow(forecast_start_times), function(i, forecast_start_times){
  if(forecast_start_times$date[i] %in% s3_stage2_parquet$ls()){
    if(forecast_start_times$dir[i] %in% s3_stage2_parquet$ls(forecast_start_times$date[i])){
      exiting_files <- length(s3_stage2_parquet$ls(forecast_start_times$dir[i]))
      if(generate_netcdf){
        if(write_s3){
          if(forecast_start_times$date[i] %in% s3_stage2_ncdf$ls()){
            if(forecast_start_times$dir[i] %in% s3_stage2_ncdf$ls(forecast_start_times$date[i])){
          exiting_files <- min(c(exiting_files, length(s3_stage2_ncdf$ls(forecast_start_times$dir[i]))))
            }else{
              exiting_files <- as.integer(0)
            }
          }else{
            exiting_files <- as.integer(0)
          }
        }else{
          exiting_files <- min(c(exiting_files, length(fs::dir_ls(file.path(s3_stage2_ncdf, forecast_start_times$dir[i])))))
        }
      }
    }else{
      exiting_files <- NA
    }
  }else{
    exiting_files <- NA
  }
  return(exiting_files)
}, forecast_start_times = forecast_start_times)

files_present <- tibble::tibble(files_present = files_present)
forecast_start_times <- bind_cols(forecast_start_times, files_present) 

forecast_start_times <- forecast_start_times |> 
  dplyr::filter(is.na(files_present) | files_present == 0 | reprocess_all)

#future::plan("future::multisession", workers = 4)
if(nrow(forecast_start_times) > 0){
  purrr::walk(1:nrow(forecast_start_times),
              function(i, forecast_start_times, df, model_name, base_dir){
                
                s3_stage2_parquet$CreateDir(forecast_start_times$dir[i])
                
                d1 <- df |> 
                  dplyr::filter(start_date == as.character(forecast_start_times$date[i]),
                                variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
                                #site_id == forecast_start_times$site_id[i],
                                cycle == as.integer(forecast_start_times$cycle[i])) |> 
                  select(-c("start_date", "cycle")) |>  
                  collect() |> 
                  disaggregate_fluxes() |> 
                  add_horizon0_time() |> 
                  convert_precip2rate() |> 
                  convert_temp2kelvin() |> 
                  convert_rh2proportion() |> 
                  disaggregate2hourly() |>
                  standardize_names_cf() |> 
                  correct_solar_geom()
                
                fname <- paste0(parquet_file_basename,"_",forecast_start_times$date[i],"_", forecast_start_times$cycle[i],".parquet")
                
                if(write_s3){
                  arrow::write_parquet(d1, sink = s3_stage2_parquet$path(file.path(forecast_start_times$dir[i],fname)))
                }else{
                  arrow::write_parquet(d1, sink = file.path(base_dir,"stage2", "parquet",forecast_start_times$dir[i],fname))
                }
                
                if(generate_netcdf){
                  site_list <- unique(d1$site_id)
                  
                  for(j in 1:length(site_list)){
                    d1 |> 
                      dplyr::filter(site_id == site_list[j]) |> 
                      standardize_names_cf() |> 
                      write_noaa_gefs_netcdf(dir = file.path(s3_stage2_ncdf_local, forecast_start_times$dir[i], site_list[j]), model_name = model_name)
                    files <- fs::dir_ls(file.path(s3_stage2_ncdf_local, forecast_start_times$dir[i],site_list[j]))
                    
                    if(write_s3){
                      readRenviron("~/.Renviron")
                      #arrow::copy_files(from = file.path(s3_stage2_ncdf_local, forecast_start_times$dir[i],site_list[j]), to = s3_stage2_ncdf$path(file.path(forecast_start_times$dir[i],site_list[j])))
                      
                      purrr::map(files, function(file){
                        aws.s3::put_object( file = file, 
                                            object = file.path("noaa/gefs-v12/stage2/ncdf", forecast_start_times$dir[i],site_list[j],basename(file)), 
                                            bucket = "neon4cast-drivers")
                        fs::file_delete(file)
                        
                      })
                    }
                  }
                }
              },
              df = df,
              forecast_start_times= forecast_start_times,
              model_name = model_name,
              base_dir = base_dir
  )
}