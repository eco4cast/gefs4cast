
# Allow user-configured bands

gefs_forecast <- function(date = "20220314",
                          cycle = "00",    # 00, 06, 12, 18 hr issued
                          series = "atmos",
                          set = "pgrb2a", # or pgrb2b for less common vars
                          res = "0p50", # half 0.50 degree resolution,
                          n_ensemble = 30,
                          ens_avg = FALSE,
                          max_horizon = 840,
                          base = "https://noaa-gefs-pds.s3.amazonaws.com/"
) {
  horizon1 <- stringr::str_pad(seq(0,240,by=3), 3, pad="0")
  horizon2 <- stringr::str_pad(seq(246,840,by=6), 3, pad="0")
  horizon <- c(horizon1, horizon2)
  
  ensemble <-  paste0("gep", stringr::str_pad(1:n_ensemble, 2, pad="0"))
  ensemble <- c("geavg","gec00", ensemble)
  
  cases <- expand.grid(horizon, ensemble) |> 
    stats::setNames(c("horizon", "ensemble")) |>
    dplyr::filter(!(ensemble == "gec00" & as.numeric(as.character(horizon)) > 384)) |>
    dplyr::filter(!(cycle != "00" & as.numeric(as.character(horizon)) > 384)) |> 
    dplyr::filter(as.numeric(as.character(horizon)) <= max_horizon) |> 
    dplyr::mutate(vars = ifelse(horizon != "000",
                                "-b 57 -b 63 -b 64 -b 67 -b 68 -b 69 -b 78 -b 79",
                                "-b 57 -b 64 -b 65 -b 66 -b 67")) |> 
    dplyr::mutate(ens_avg = ens_avg) |> 
    dplyr::filter((ens_avg == TRUE & ensemble == "geavg") | 
                    (ens_avg == FALSE & ensemble != "geavg")) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(url = gefs_url(horizon, date, NN=ensemble, cycle)) |> 
    dplyr::select(url, vars)
  cases
}

