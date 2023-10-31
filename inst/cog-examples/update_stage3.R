site_list <- readr::read_csv(paste0("https://github.com/eco4cast/",
                                    "neon4cast-noaa-download/",
                                    "raw/master/noaa_download_site_list.csv"),
                             show_col_types = FALSE) |> dplyr::pull(site_id)

future::plan("future::multisession", workers = 8)

furrr::future_walk(site_list, function(curr_site_id){

  print(curr_site_id)

  s3 <- arrow::s3_bucket("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/stage3",
                         endpoint_override = "sdsc.osn.xsede.org",
                         access_key= Sys.getenv("OSN_KEY"),
                         secret_key= Sys.getenv("OSN_SECRET"))

  stage3_df <- arrow::open_dataset(s3) |>
    dplyr::filter(site_id == curr_site_id) |>
    dplyr::collect()

  max_date <- stage3_df |>
    dplyr::summarise(max = as.character(lubridate::as_date(max(datetime)))) |>
    dplyr::pull(max)

  s3_pseudo <- arrow::s3_bucket("bio230014-bucket01/neon4cast-drivers/noaa/gefs-v12/pseudo",
                                endpoint_override = "sdsc.osn.xsede.org",
                                access_key= Sys.getenv("OSN_KEY"),
                                secret_key= Sys.getenv("OSN_SECRET"))

  vars <- names(stage3_df)

  df <- arrow::open_dataset(s3_pseudo) |>
    dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::filter(site_id == curr_site_id,
                  reference_datetime >= "2023-10-30") |>
    dplyr::collect()

  if(nrow(df) > 0){

    df |>
      to_hourly(use_solar_geom = TRUE, psuedo = TRUE) |>
      dplyr::bind_rows(stage3_df) |>
      dplyr::arrange(variable, datetime, ensemble) |>
      dplyr::distinct() |>
      arrow::write_dataset(path = s3, partitioning = "site_id")
  }
})
