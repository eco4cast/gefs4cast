disaggregate_fluxes <- function(df){
  
  var_order <- names(df)
  df |> 
    mutate(flux = ifelse(variable %in% c("APCP","DSWRF","DLWRF"), 1, 0),
           start = ifelse(flux == 1, gsub("^(\\d+).*", "\\1",forecast_valid), NA),
           end = ifelse(flux == 1, gsub("\\d+-(\\d+).*",  "\\1", forecast_valid), NA),
           diff = ifelse(flux == 1, as.numeric(end) - as.numeric(start), NA),
           time = lubridate::as_datetime(ifelse(is.na(time) & forecast_valid == "anl", start_time, time)))|> 
    group_by(site_id, variable, height, horizon, ensemble, start_time, time, longitude, latitude) |> 
    mutate(nrows = length(predicted),
           predicted = ifelse(diff == 6 & nrows == 2 & variable != "APCP", 2 * last(predicted) - first(predicted), predicted),
           predicted = ifelse(diff == 6 & nrows == 2 & variable == "APCP", last(predicted) - first(predicted), predicted),
           time = lubridate::as_datetime(ifelse(diff == 6 & nrows == 2, time + lubridate::hours(3), time)),
           horizon = ifelse(diff == 6 & nrows == 2, horizon + 3, horizon),
           start = ifelse(diff == 6 & nrows == 2, as.numeric(start) + 3, as.numeric(start)),
           forecast_valid = ifelse(flux == 1 & variable != "APCP", paste0(start,"-",end," hour ave fcst"), forecast_valid),
           forecast_valid = ifelse(flux == 1 & variable == "APCP", paste0(start,"-",end," hour acc fcst"), forecast_valid)) |> 
    ungroup() |> 
    select(all_of(var_order))
}


add_horizon0_time <- function(df){
  df |> 
    mutate(time = lubridate::as_datetime(ifelse(is.na(time) & forecast_valid == "anl", start_time, time)))
}

convert_precip2rate <- function(df){
  var_order <- names(df)
  df |> 
    mutate(start = ifelse(variable == "APCP", gsub("^(\\d+).*", "\\1",forecast_valid), NA),
           end = ifelse(variable == "APCP", gsub("\\d+-(\\d+).*",  "\\1", forecast_valid), NA),
           diff = ifelse(variable == "APCP", as.numeric(end) - as.numeric(start), NA),
           predicted = ifelse(variable == "APCP", predicted / (diff * 60 * 60), predicted),
           variable = ifelse(variable == "APCP", "PRATE", variable),
           forecast_valid = ifelse(variable == "PRATE", paste0(start,"-",end," hour ave fcst"), forecast_valid)) |> 
    select(all_of(var_order))
}

disaggregate2hourly <- function(df){
  
  height_table <- df |> 
    select(variable, height) |> 
    distinct()
  
  ensemble_maxtime <- df |> 
    group_by(site_id, ensemble, start_time) |> 
    summarise(max_time = max(time), .groups = "drop")
  
  
  ensembles <- unique(df$ensemble)
  sites <- unique(df$site_id)
  time <- seq(min(df$time), max(df$time), by = "1 hour")
  start_time <- unique(df$start_time)
  
  full_time <- expand.grid(sites, ensembles, time, start_time) |> 
    rename(site_id = Var1,
           ensemble = Var2,
           time = Var3,
           start_time = Var4) |> 
    mutate(time = lubridate::as_datetime(time)) |> 
    arrange(site_id, ensemble, start_time, time) |> 
    dplyr::left_join(ensemble_maxtime, by = c("site_id","ensemble", "start_time")) |> 
    dplyr::filter(time <= max_time) |> 
    dplyr::select(-c("max_time"))
  
  var_order <- names(df)
  
  df |> 
    select(site_id, ensemble, time, variable, predicted, longitude, latitude, start_time) |> 
    tidyr::pivot_wider(names_from = variable, values_from = predicted) |> 
    dplyr::right_join(full_time, by = c("site_id", "ensemble", "time", "start_time")) |> 
    arrange(site_id, ensemble, time) |> 
    tidyr::pivot_longer(-c("site_id", "ensemble", "time", "longitude", "latitude", "start_time"), names_to = "variable", values_to = "predicted") |> 
    group_by(variable, ensemble) |> 
    mutate(predicted = imputeTS::na_interpolation(predicted, option = "linear"),
           horizon = as.numeric(time - start_time) / (60 * 60)) |> 
    arrange(site_id, ensemble, variable, time) |> 
    tidyr::fill(c("longitude","latitude"), .direction = "down") |> 
    mutate(flux = ifelse(variable %in% c("PRATE","DSWRF","DLWRF"), 1, 0),
           forecast_valid = ifelse(flux == 1, paste0(horizon,"-",horizon + 1," hour ave fcst"), paste0(horizon," hour fcst")),
           forecast_valid = ifelse(horizon == 0, "anl", forecast_valid)) |> 
    dplyr::left_join(height_table, by = "variable") |>   
    select(all_of(var_order)) |> 
    ungroup()
  
}

correct_solar_geom <- function(df){
  
  var_order <- names(df)
  d <- df |> 
    dplyr::mutate(hour = lubridate::hour(time),
                  date = lubridate::as_date(time),
                  doy = lubridate::yday(time) + hour/24,
                  lon = ifelse(longitude < 0, 360 + longitude,longitude),
                  rpot = downscale_solar_geom(doy, lon, latitude)) |>  # hourly sw flux calculated using solar geometry
    dplyr::group_by(site_id, ensemble, longitude, latitude, start_time, date) |> 
    dplyr::mutate(avg.rpot = mean(rpot, na.rm = TRUE),
                  avg.SW = mean(predicted, na.rm = TRUE))|> # daily sw mean from solar geometry
    dplyr::ungroup() |>
    dplyr::mutate(predicted = ifelse(variable == "DSWRF", rpot * (avg.SW/avg.rpot),predicted)) |> 
    select(all_of(var_order))
}

write_noaa_gefs_netcdf <- function(df, dir, model_name, add_directory){
  
  df <- df |> 
    mutate(ensemble = ifelse(ensemble == 31, 0, ensemble),
           unit = NA,
           unit = ifelse(variable == "TMP", "K", unit),
           unit = ifelse(variable == "PRES", "Pa", unit),
           unit = ifelse(variable == "RH", "1", unit),
           unit = ifelse(variable == "DLWRF", "Wm-2", unit),
           unit = ifelse(variable == "DSWRF", "Wm-2", unit),
           unit = ifelse(variable == "PRATE", "kgm-2s-1", unit),
           unit = ifelse(variable == "VGRD", "ms-1", unit),
           unit = ifelse(variable == "UGRD", "ms-1", unit),
           variable = ifelse(variable == "TMP", "air_temperature", variable),
           variable = ifelse(variable == "PRES", "air_pressure", variable),
           variable = ifelse(variable == "RH", "relative_humidity", variable),
           variable = ifelse(variable == "DLWRF", "surface_downwelling_longwave_flux_in_air", variable),
           variable = ifelse(variable == "DSWRF", "surface_downwelling_shortwave_flux_in_air", variable),
           variable = ifelse(variable == "PRATE", "precipitation_flux", variable),
           variable = ifelse(variable == "VGRD", "eastward_wind", variable),
           variable = ifelse(variable == "UGRD", "northward_wind", variable))
  
  files <- df |> 
    distinct(ensemble,site_id,start_time,latitude,longitude, .keep_all = FALSE)
  
  for(i in 1:nrow(files)){
    
    curr_df <- df |> 
      filter(site_id == files$site_id[i],
             ensemble == files$ensemble[i],
             start_time == files$start_time[i]) |> 
      mutate(longitude = ifelse(longitude < 0, longitude + 360, longitude))
    
    max_time <- max(curr_df$time)
    
    if(add_directory){
      cycle <- stringr::str_pad(lubridate::hour(lubridate::as_datetime((files$start_time[i]))), 2, side = "left", pad = "0")
      output_dir <- file.path(dir, model_name, files$site_id[i], lubridate::as_date(files$start_time[i]), cycle)
    }else{
      output_dir <- dir
    }
    fs::dir_create(output_dir)
    
    output_file <- paste0(model_name,"_",files$site_id[i],"_", format(files$start_time[i], "%Y-%m-%dT%H"),"_",
                          format(max_time, "%Y-%m-%dT%H"),"_ens",stringr::str_pad(files$ensemble[i], 2, "left", 0),".nc")
    
    #Define dimensions:
    time_dim <- ncdf4::ncdim_def(name="time",
                                 units = paste("hours since", format(files$start_time[i], "%Y-%m-%d %H:%M")),
                                 unique(curr_df$horizon), #GEFS forecast starts 6 hours from start time
                                 create_dimvar = TRUE)
    lat_dim <- ncdf4::ncdim_def("latitude", "degree_north", files$latitude[i], create_dimvar = TRUE)
    lon_dim <- ncdf4::ncdim_def("longitude", "degree_east", files$longitude[i], create_dimvar = TRUE)
    
    dimensions_list <- list(time_dim, lat_dim, lon_dim)
    
    varnames <- curr_df |> 
      distinct(variable,unit,.keep_all = FALSE)
    
    nc_var_list <- list()
    for (j in 1:nrow(varnames)) { #Each ensemble member will have data on each variable stored in their respective file.
      nc_var_list[[j]] <- ncdf4::ncvar_def(varnames$variable[j], varnames$unit[j], dimensions_list, missval=NaN)
    }
    
    nc_flptr <- ncdf4::nc_create(file.path(output_dir, output_file), nc_var_list, verbose = FALSE)
    
    #For each variable associated with that ensemble
    for (j in 1:nrow(varnames)) {
      data <- curr_df |> 
        filter(variable == varnames$variable[j]) |> 
        pull(predicted)
      # "j" is the variable number.  "i" is the ensemble number. Remember that each row represents an ensemble
      ncdf4::ncvar_put(nc_flptr, nc_var_list[[j]], data)
    }
    
    ncdf4::nc_close(nc_flptr)  #Write to the disk/storage
  }
}

average_ensembles <- function(df){
  df |> 
    group_by(site_id, ensemble, time, variable, longitude, latitude, start_time, horizon) |> 
    summarize(predicted = mean(predicted, na.rm = TRUE), .groups = "drop") |> 
    mutate(ensemble = "AV")
}

#' Cosine of solar zenith angle
#'
#' For explanations of formulae, see http://www.itacanet.org/the-sun-as-a-source-of-energy/part-3-calculating-solar-angles/
#'
#' @author Alexey Shiklomanov
#' @param doy Day of year
#' @param lat Latitude
#' @param lon Longitude
#' @param dt Timestep
#' @noRd
#' @param hr Hours timestep
#' @return `numeric(1)` of cosine of solar zenith angle
#' @export
cos_solar_zenith_angle <- function(doy, lat, lon, dt, hr) {
  et <- equation_of_time(doy)
  merid  <- floor(lon / 15) * 15
  merid[merid < 0] <- merid[merid < 0] + 15
  lc     <- (lon - merid) * -4/60  ## longitude correction
  tz     <- merid / 360 * 24  ## time zone
  midbin <- 0.5 * dt / 86400 * 24  ## shift calc to middle of bin
  t0   <- 12 + lc - et - tz - midbin  ## solar time
  h    <- pi/12 * (hr - t0)  ## solar hour
  dec  <- -23.45 * pi / 180 * cos(2 * pi * (doy + 10) / 365)  ## declination
  cosz <- sin(lat * pi / 180) * sin(dec) + cos(lat * pi / 180) * cos(dec) * cos(h)
  cosz[cosz < 0] <- 0
  return(cosz)
}

#' Equation of time: Eccentricity and obliquity
#'
#' For description of calculations, see https://en.wikipedia.org/wiki/Equation_of_time#Calculating_the_equation_of_time
#'
#' @author Alexey Shiklomanov
#' @param doy Day of year
#' @noRd
#' @return `numeric(1)` length of the solar day, in hours.

equation_of_time <- function(doy) {
  stopifnot(doy <= 367)
  f      <- pi / 180 * (279.5 + 0.9856 * doy)
  et     <- (-104.7 * sin(f) + 596.2 * sin(2 * f) + 4.3 *
               sin(4 * f) - 429.3 * cos(f) - 2 *
               cos(2 * f) + 19.3 * cos(3 * f)) / 3600  # equation of time -> eccentricity and obliquity
  return(et)
}

downscale_solar_geom <- function(doy, lon, lat) {
  
  dt <- median(diff(doy)) * 86400 # average number of seconds in time interval
  hr <- (doy - floor(doy)) * 24 # hour of day for each element of doy
  
  ## calculate potential radiation
  cosz <- cos_solar_zenith_angle(doy, lat, lon, dt, hr)
  rpot <- 1366 * cosz
  return(rpot)
}