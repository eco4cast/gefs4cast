library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

source("/home/rstudio/gefs4cast/inst/examples/temporal_disaggregation.R")

base_dir <- "/home/rstudio/test_processing/noaa/gefs-v12/"
generate_netcdf <- TRUE

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
model_name <- "NOAAGEFS_1hr_stacked"
generate_netcdf <- TRUE
write_s3 <- TRUE

s3_stage1 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage1", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)

s3_stage3 <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/", 
                              endpoint_override =  "data.ecoforecast.org")
s3_stage3$CreateDir("stage3/parquet")
if(generate_netcdf) s3_stage3$CreateDir("stage3/ncdf")

s3_stage3_parquet <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage3/parquet", 
                                      endpoint_override =  "data.ecoforecast.org")

if(generate_netcdf){
  s3_stage3_ncdf_local <- file.path(base_dir,"stage3", "ncdf")
  s3_stage3_ncdf <- arrow::s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage3/ncdf", 
                                     endpoint_override =  "data.ecoforecast.org")
  
  fs::dir_create(s3_stage3_ncdf_local)
}

df <- arrow::open_dataset(s3_stage1, partitioning = c("start_date", "cycle"))

sites <- df |> 
  dplyr::filter(start_date == "2020-09-25",
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

purrr::walk(sites, function(site, base_dir){
  fname <- paste0("stage3-",site,".parquet")
  if(site %in% s3_stage3_parquet$ls()){
    d <- arrow::read_parquet(s3_stage3_parquet$path(file.path(site, fname))) %>% 
      mutate(start_date = lubridate::as_date(time))
    max_start_date <- max(d$start_date)
    d2 <- d %>% 
      filter(start_date != max_start_date)
    date_range <- as.character(seq(max_start_date, Sys.Date(), by = "1 day"))
  }else{
    date_range <- as.character(seq(lubridate::as_date("2020-09-25"), Sys.Date(), by = "1 day"))
    d2 <- NULL
  }
  
  d1 <- df |> 
    filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
           start_date %in% date_range,
           site_id == site,
           horizon %in% c(0,3)) |> 
    select(-c("start_date", "cycle")) |>
    collect() |> 
    distinct() |> 
    disaggregate_fluxes() |>  
    add_horizon0_time() |> 
    convert_precip2rate() |>
    filter(horizon < 6) |> 
    mutate(start_time = min(start_time)) |> 
    disaggregate2hourly() |>
    standardize_names_cf() |> 
    dplyr::bind_rows(d2) |>
    arrange(site_id, time, variable, ensemble)
  
  #NEED TO UPDATE TO WRITE TO S3
  arrow::write_parquet(d1, sink = s3_stage3_parquet$path(file.path(site, fname)))
  
  if(generate_netcdf){
    d1 |> 
      standardize_names_cf() |> 
      write_noaa_gefs_netcdf(dir = file.path(s3_stage3_ncdf_local, site), model_name = model_name)
    files <- fs::dir_ls(file.path(s3_stage3_ncdf_local, site))
    
    if(write_s3){
      readRenviron("~/.Renviron")
      purrr::map(files, function(file){
        aws.s3::put_object( file = file, 
                            object = file.path("noaa/gefs-v12/stage3/ncdf", site,basename(file)), 
                            bucket = "neon4cast-drivers") 
        fs::file_delete(file)
      })
    }
  }
},
base_dir = base_dir
)

#d1 |> 
#  dplyr::filter(ensemble <= 31) %>% 
#  ggplot(aes(x = time, y = predicted, group = ensemble))  +
#  geom_line() +
#  facet_wrap(~variable, scale = "free")

#ggsave(p, filename = paste0("/home/rstudio/", sites[i],".pdf"), device = "pdf", height = 6, width = 12)




