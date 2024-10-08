disaggregate_fluxes <- function(df){
  
  var_order <- names(df)
  df |> 
    mutate(flux = ifelse(variable %in% c("APCP","DSWRF","DLWRF"), 1, 0),
           start = ifelse(flux == 1, gsub("^(\\d+).*", "\\1",forecast_valid), NA),
           end = ifelse(flux == 1, gsub("\\d+-(\\d+).*",  "\\1", forecast_valid), NA),
           diff = ifelse(flux == 1, as.numeric(end) - as.numeric(start), NA),
           datetime = lubridate::as_datetime(ifelse(is.na(datetime) & forecast_valid == "anl", reference_datetime, datetime)))|> 
    group_by(site_id, variable, height, horizon, family, parameter, reference_datetime, datetime, longitude, latitude) |> 
    mutate(nrows = length(prediction),
           prediction = ifelse(diff == 6 & nrows == 2 & variable != "APCP", 2 * last(prediction) - first(prediction), prediction),
           prediction = ifelse(diff == 6 & nrows == 2 & variable == "APCP", last(prediction) - first(prediction), prediction),
           datetime = lubridate::as_datetime(ifelse(diff == 6 & nrows == 2, datetime + lubridate::hours(3), datetime)),
           horizon = ifelse(diff == 6 & nrows == 2, horizon + 3, horizon),
           start = ifelse(diff == 6 & nrows == 2, as.numeric(start) + 3, as.numeric(start)),
           forecast_valid = ifelse(flux == 1 & variable != "APCP", paste0(start,"-",end," hour ave fcst"), forecast_valid),
           forecast_valid = ifelse(flux == 1 & variable == "APCP", paste0(start,"-",end," hour acc fcst"), forecast_valid)) |> 
    ungroup() |> 
    select(all_of(var_order))
}


add_horizon0_time <- function(df){
  df |> 
    mutate(datetime = lubridate::as_datetime(ifelse(is.na(datetime) & forecast_valid == "anl", reference_datetime, datetime)))
}

convert_precip2rate <- function(df){
  var_order <- names(df)
  df |> 
    mutate(start = ifelse(variable == "APCP", gsub("^(\\d+).*", "\\1",forecast_valid), NA),
           end = ifelse(variable == "APCP", gsub("\\d+-(\\d+).*",  "\\1", forecast_valid), NA),
           diff = ifelse(variable == "APCP", as.numeric(end) - as.numeric(start), NA),
           prediction = ifelse(variable == "APCP", prediction / (diff * 60 * 60), prediction),
           variable = ifelse(variable == "APCP", "PRATE", variable),
           forecast_valid = ifelse(variable == "PRATE", paste0(start,"-",end," hour ave fcst"), forecast_valid)) |> 
    select(all_of(var_order))
}

convert_temp2kelvin <- function(df){
  df |> 
    mutate(prediction = ifelse(variable == "TMP", prediction + 273, prediction))
}

convert_rh2proportion <- function(df){
  df |> 
    mutate(prediction = ifelse(variable == "RH", prediction/100, prediction))
}

disaggregate2hourly <- function(df){
  
  height_table <- df |> 
    select(variable, height) |> 
    distinct()
  
  ensemble_maxtime <- df |> 
    group_by(site_id, family, parameter, reference_datetime) |> 
    summarise(max_time = max(datetime), .groups = "drop")
  
  
  ensembles <- unique(df$parameter)
  sites <- unique(df$site_id)
  datetime <- seq(min(df$datetime), max(df$datetime), by = "1 hour")
  reference_datetime <- unique(df$reference_datetime)
  
  full_time <- expand.grid(sites, ensembles, datetime, reference_datetime) |> 
    rename(site_id = Var1,
           parameter = Var2,
           datetime = Var3,
           reference_datetime = Var4) |> 
    mutate(datetime = lubridate::as_datetime(datetime)) |> 
    arrange(site_id, parameter, reference_datetime, datetime) |> 
    dplyr::left_join(ensemble_maxtime, by = c("site_id","parameter", "reference_datetime")) |> 
    dplyr::filter(datetime <= max_time) |> 
    dplyr::select(-c("max_time"))
  
  var_order <- names(df)
  
  df |> 
    select(site_id, family, parameter, datetime, variable, prediction, longitude, latitude, reference_datetime) |> 
    tidyr::pivot_wider(names_from = variable, values_from = prediction) |> 
    dplyr::right_join(full_time, by = c("site_id", "parameter", "datetime", "reference_datetime", "family")) |> 
    arrange(site_id, family, parameter, datetime) |> 
    group_by(site_id, family, parameter)  |> 
    tidyr::fill(c("PRATE","DSWRF","DLWRF"), .direction = "down") |> 
    mutate(PRES =  imputeTS::na_interpolation(PRES, option = "linear"),
           RH =  imputeTS::na_interpolation(RH, option = "linear"),
           TMP =  imputeTS::na_interpolation(TMP, option = "linear"),
           UGRD =  imputeTS::na_interpolation(UGRD, option = "linear"),
           VGRD =  imputeTS::na_interpolation(VGRD, option = "linear")) |>
    ungroup() |>
    tidyr::pivot_longer(-c("site_id", "family", "parameter", "datetime", "longitude", "latitude", "reference_datetime"), names_to = "variable", values_to = "prediction") |> 
    group_by(site_id, variable, family, parameter) |> 
    mutate(horizon = as.numeric(datetime - reference_datetime) / (60 * 60)) |> 
    arrange(site_id, family, parameter, variable, datetime) |> 
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
    dplyr::mutate(hour = lubridate::hour(datetime),
                  date = lubridate::as_date(datetime),
                  doy = lubridate::yday(datetime) + hour/24,
                  lon = ifelse(longitude < 0, 360 + longitude,longitude),
                  rpot = downscale_solar_geom(doy, lon, latitude)) |>  # hourly sw flux calculated using solar geometry
    dplyr::group_by(site_id, family, parameter, longitude, latitude, reference_datetime, date, variable) |> 
    dplyr::mutate(avg.rpot = mean(rpot, na.rm = TRUE),
                  avg.SW = mean(prediction, na.rm = TRUE))|> # daily sw mean from solar geometry
    dplyr::ungroup() |>
    dplyr::mutate(prediction = ifelse(variable %in% c("DSWRF","surface_downwelling_shortwave_flux_in_air") & avg.rpot > 0.0, rpot * (avg.SW/avg.rpot),prediction)) |> 
    select(all_of(var_order))
}

write_noaa_gefs_netcdf <- function(df, dir, model_name, add_directory){
  
  df <- df |> 
    mutate(parameter = ifelse(parameter == 31, 0, parameter),
           unit = NA,
                  unit = ifelse(variable == "air_temperature", "K", unit),
                  unit = ifelse(variable == "air_pressure", "Pa", unit),
                  unit = ifelse(variable == "relative_humidity", "1", unit),
                  unit = ifelse(variable == "surface_downwelling_longwave_flux_in_air", "Wm-2", unit),
                  unit = ifelse(variable == "surface_downwelling_shortwave_flux_in_air", "Wm-2", unit),
                  unit = ifelse(variable == "precipitation_flux", "kgm-2s-1", unit),
                  unit = ifelse(variable == "eastward_wind", "ms-1", unit),
                  unit = ifelse(variable == "northward_wind", "ms-1", unit),
                  unit = ifelse(variable == "precipitation_amount", "kgm-2", unit))
  
  files <- df |> 
    distinct(family, parameter,site_id,reference_datetime,latitude,longitude, .keep_all = FALSE)
  
  for(i in 1:nrow(files)){
    
    curr_df <- df |> 
      dplyr::filter(site_id == files$site_id[i],
             parameter == files$parameter[i],
             reference_datetime == files$reference_datetime[i]) |>
      mutate(horizon = as.numeric(datetime - reference_datetime) / (60 * 60)) |> 
      mutate(longitude = ifelse(longitude < 0, longitude + 360, longitude))
    
    max_time <- max(curr_df$datetime)
    
    output_dir <- dir
    fs::dir_create(output_dir)
    
    output_file <- paste0(model_name,"_",files$site_id[i],"_", format(files$reference_datetime[i], "%Y-%m-%dT%H"), "_ens",stringr::str_pad(files$parameter[i], 2, "left", 0),".nc")
    
    #Define dimensions:
    time_dim <- ncdf4::ncdim_def(name="time",
                                 units = paste("hours since", format(files$reference_datetime[i], "%Y-%m-%d %H:%M")),
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
        dplyr::filter(variable == varnames$variable[j]) |> 
        pull(prediction)
      # "j" is the variable number.  "i" is the ensemble number. Remember that each row represents an ensemble
      ncdf4::ncvar_put(nc_flptr, nc_var_list[[j]], data)
    }
    
    ncdf4::nc_close(nc_flptr)  #Write to the disk/storage
  }
}

standardize_names_cf <- function(df){
  df |> 
    mutate(variable = ifelse(variable == "TMP", "air_temperature", variable),
           variable = ifelse(variable == "PRES", "air_pressure", variable),
           variable = ifelse(variable == "RH", "relative_humidity", variable),
           variable = ifelse(variable == "DLWRF", "surface_downwelling_longwave_flux_in_air", variable),
           variable = ifelse(variable == "DSWRF", "surface_downwelling_shortwave_flux_in_air", variable),
           variable = ifelse(variable == "PRATE", "precipitation_flux", variable),
           variable = ifelse(variable == "VGRD", "eastward_wind", variable),
           variable = ifelse(variable == "UGRD", "northward_wind", variable),
           variable = ifelse(variable == "APCP", "precipitation_amount", variable))
}

average_ensembles <- function(df){
  df |> 
    group_by(site_id, family, parameter, datetime, variable, longitude, latitude, reference_datetime, horizon) |> 
    summarize(prediction = mean(prediction, na.rm = TRUE), .groups = "drop") |> 
    mutate(parameter = "AV")
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
