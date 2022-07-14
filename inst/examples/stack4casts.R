library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

base_dir <- "/home/rstudio/test_processing/NOAAGEFS_1hr_stacked"
generate_netcdf <- TRUE

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)

df <- arrow::open_dataset(s3, partitioning = c("start_date", "cycle"))

sites <- df |> 
  dplyr::filter(start_date == "2020-09-25",
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

sites <- sites[1:3]

purrr::walk(sites, function(site, base_dir){
  site_dir <- file.path(base_dir, site)
  fs::dir_create(site_dir)
  site_file <- paste0(site_dir,"/", site,".parquet")
  if(fs::file_exists(site_file)){
    d <- arrow::read_parquet(site_file) %>% 
      mutate(start_date = lubridate::as_date(time))
    max_start_date <- max(d$start_date)
    d2 <- d %>% 
      filter(start_date != max_start_date)
    date_range <- as.character(seq(max_start_date, Sys.Date(), by = "1 day"))
  }else{
    date_range <- as.character(seq(lubridate::as_date("2020-09-25"), Sys.Date(), by = "1 day"))
    d2 <- NULL
  }
  
  forecast <- df |> 
    filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
           start_date %in% date_range,
           site_id == sites[i],
           horizon %in% c(0,3)) |> 
    select(-c("start_date", "cycle")) |>
    collect() |> 
    distinct()
  
  forecast <- forecast |> 
    disaggregate_fluxes() |>  
    add_horizon0_time() |> 
    convert_precip2rate() |>
    filter(horizon < 6) |> 
    mutate(start_time = min(start_time)) |> 
    disaggregate2hourly() |>
    dplyr::bind_rows(d2) |>
    arrange(site_id, time, variable, ensemble)
  
  #NEED TO UPDATE TO WRITE TO S3
  arrow::write_parquet(forecast, sink = site_file)
  
  if(generate_netcdf){
    #NEED TO UPDATE TO MOVE TO S3
    write_noaa_gefs_netcdf(forecast, dir = site_dir, model_name = "NOAAGEFS_1hr_stacked")
    netcdf_files <- fs::dir_ls(site_dir, recurse = TRUE, regexp = "[.]nc$")
    #Add copy netcdf_files to bucket
  }
  
  
},
base_dir = base_dir
)

#p <- forecast |> 
#  filter(ensemble <= 31) %>% 
#  ggplot(aes(x = time, y = predicted, group = ensemble))  +
#  geom_line() +
#  facet_wrap(~variable, scale = "free")

#ggsave(p, filename = paste0("/home/rstudio/", sites[i],".pdf"), device = "pdf", height = 6, width = 12)




