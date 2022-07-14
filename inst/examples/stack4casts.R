library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)

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

purrr::walk(sites, function(site, dir){
  site_file <- paste0(dir, "/", site,".parquet")
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
    arrange(site_id, time, variable, ensemble) |>
    arrow::write_parquet(sink = site_file)
},
dir = "/home/rstudio"
)
  
  #p <- forecast |> 
  #  filter(ensemble <= 31) %>% 
  #  ggplot(aes(x = time, y = predicted, group = ensemble))  +
  #  geom_line() +
  #  facet_wrap(~variable, scale = "free")
  
  #ggsave(p, filename = paste0("/home/rstudio/", sites[i],".pdf"), device = "pdf", height = 6, width = 12)
  



