to_hourly <- function(i, site_list, date, use_solar_geom = TRUE, stage1 = "bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage1", endpoint_override = "https://sdsc.osn.xsede.org", site_id_partition = TRUE){

  if(!identical(names(site_list), c("site_name","site_id","latitude","longitude"))){
    stop("site list needs to have the following columns in this order:'site_name','site_id','latitude','longitude')")
  }

  if(site_id_partition = TRUE){
    s3 <- arrow::s3_bucket(paste0(stage1,"/reference_datetime=",date,"/site_id=",site_list$site_id[i]), endpoint_override,
                           anonymous = TRUE)
  }else{
    s3 <- arrow::s3_bucket(paste0(stage1,"/reference_datetime=",date), endpoint_override,
                           anonymous = TRUE)
  }

  df <- arrow::open_dataset(s3) |> dplyr::collect()

  df <- df |>
    dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::arrange(variable, datetime) |>
    dplyr::mutate(reference_datetime = lubridate::as_datetime(paste0(date, " 00:00:00")),
                  site_id = site_list$site_id[i],
                  horizon = as.numeric(horizon) / (60 * 60))


  var_order <- names(df)

  df <- df |>
    dplyr::mutate(flux = ifelse(variable %in% c("APCP","DSWRF","DLWRF"), 1, 0),
                  start = ifelse((horizon %% 2) != 0 & horizon != 0, horizon - 3, horizon - 6),
                  end =  horizon,
                  diff = ifelse(flux == 1, as.numeric(end) - as.numeric(start), NA),
                  horizon ) |>
    dplyr::group_by(site_id, variable, start, family, ensemble, reference_datetime, datetime, cycle) |>
    dplyr::mutate(nrows = length(prediction),
                  prediction = ifelse(diff == 6 & nrows == 2 & variable != "APCP", 2 * last(prediction) - first(prediction), prediction),
                  prediction = ifelse(diff == 6 & nrows == 2 & variable == "APCP", last(prediction) - first(prediction), prediction)) |>
    dplyr::ungroup() |>
    dplyr::select(all_of(var_order))

  df <- df |>
    dplyr::mutate(flux = ifelse(variable %in% c("APCP","DSWRF","DLWRF"), 1, 0),
                  start = ifelse((horizon %% 2) != 0 & horizon != 0, horizon - 3, horizon - 6),
                  end =  horizon,
                  diff = ifelse(flux == 1, as.numeric(end) - as.numeric(start), NA),
                  prediction = ifelse(variable == "APCP", prediction / (diff * 60 * 60), prediction),
                  variable = ifelse(variable == "APCP", "PRATE", variable)) |>
    dplyr::mutate(prediction = ifelse(variable == "TMP", prediction + 273, prediction)) |>
    dplyr::mutate(prediction = ifelse(variable == "RH", prediction/100, prediction))


  ensemble_maxtime <- df |>
    dplyr::group_by(site_id, family, ensemble, reference_datetime) |>
    dplyr::summarise(max_time = max(datetime), .groups = "drop")


  ensembles <- unique(df$ensemble)
  datetime <- seq(min(df$datetime), max(df$datetime), by = "1 hour")
  reference_datetime <- unique(df$reference_datetime)
  sites <- unique(df$site_id)

  full_time <- expand.grid(sites, ensembles, datetime, reference_datetime) |>
    dplyr::rename(site_id = Var1,
                  ensemble = Var2,
                  datetime = Var3,
                  reference_datetime = Var4) |>
    dplyr::mutate(datetime = lubridate::as_datetime(datetime)) |>
    dplyr::arrange(site_id, ensemble, reference_datetime, datetime) |>
    dplyr::left_join(ensemble_maxtime, by = c("site_id","ensemble", "reference_datetime")) |>
    dplyr::filter(datetime <= max_time) |>
    dplyr::select(-c("max_time"))


  df <- df |>
    select(site_id, family, ensemble, datetime, variable, prediction, reference_datetime) |>
    tidyr::pivot_wider(names_from = variable, values_from = prediction) |>
    dplyr::right_join(full_time, by = c("site_id", "ensemble", "datetime", "reference_datetime", "family")) |>
    dplyr::arrange(site_id, family, ensemble, datetime) |>
    dplyr::group_by(site_id, family, ensemble)  |>
    tidyr::fill(c("PRATE","DSWRF","DLWRF"), .direction = "up") |>
    dplyr::mutate(PRES =  imputeTS::na_interpolation(PRES, option = "linear"),
                  RH =  imputeTS::na_interpolation(RH, option = "linear"),
                  TMP =  imputeTS::na_interpolation(TMP, option = "linear"),
                  UGRD =  imputeTS::na_interpolation(UGRD, option = "linear"),
                  VGRD =  imputeTS::na_interpolation(VGRD, option = "linear"),
                  DSWRF = ifelse(use_solar_geom, DSWRF, imputeTS::na_interpolation(DSWRF, option = "linear"))) |>
    dplyr::ungroup() |>
    tidyr::pivot_longer(-c("site_id", "family", "ensemble", "datetime", "reference_datetime"), names_to = "variable", values_to = "prediction") |>
    dplyr::group_by(site_id, variable, family, ensemble) |>
    dplyr::mutate(horizon = as.numeric(datetime - reference_datetime) / (60 * 60)) |>
    dplyr::arrange(site_id, family, ensemble, variable, datetime) |>
    dplyr::select(any_of(var_order)) |>
    dplyr::ungroup() |>
    dplyr::mutate(variable = ifelse(variable == "TMP", "air_temperature", variable),
                  variable = ifelse(variable == "PRES", "air_pressure", variable),
                  variable = ifelse(variable == "RH", "relative_humidity", variable),
                  variable = ifelse(variable == "DLWRF", "surface_downwelling_longwave_flux_in_air", variable),
                  variable = ifelse(variable == "DSWRF", "surface_downwelling_shortwave_flux_in_air", variable),
                  variable = ifelse(variable == "PRATE", "precipitation_flux", variable),
                  variable = ifelse(variable == "VGRD", "eastward_wind", variable),
                  variable = ifelse(variable == "UGRD", "northward_wind", variable),
                  variable = ifelse(variable == "APCP", "precipitation_amount", variable))

  if(use_solar_geom){

    df <- df |>
      dplyr::mutate(hour = lubridate::hour(datetime),
                    date = lubridate::as_date(datetime),
                    doy = lubridate::yday(datetime) + hour/24,
                    lon = ifelse(site_list$longitude[i] < 0, 360 + site_list$longitude[i],site_list$longitude[i]),
                    rpot = downscale_solar_geom(doy, lon, site_list$latitude[i])) |>  # hourly sw flux calculated using solar geometry
      dplyr::group_by(site_id, family, ensemble, reference_datetime, date, variable) |>
      dplyr::mutate(avg.rpot = mean(rpot, na.rm = TRUE),
                    avg.SW = mean(prediction, na.rm = TRUE))|> # daily sw mean from solar geometry
      dplyr::ungroup() |>
      dplyr::mutate(prediction = ifelse(variable %in% c("DSWRF","surface_downwelling_shortwave_flux_in_air") & avg.rpot > 0.0, rpot * (avg.SW/avg.rpot),prediction)) |>
      dplyr::select(any_of(var_order))
  }

  return(df)

}

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
