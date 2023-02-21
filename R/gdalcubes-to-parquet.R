

efi_format_cubeextract <- function(dfs, sf_sites = neon_sites()) {
  sites <- sf_sites |> tibble::as_tibble() |> dplyr::select(FID, site_id)
  df <-
    purrr::list_rbind(dfs, names_to = "statistic") |>
    tibble::as_tibble() |>
    dplyr::inner_join(sites) |>
    dplyr::rename("PRES"= "band57",
                  "TMP" = "band63",
                  "RH" = "band64",
                  "UGRD" = "band67",
                  "VGRD" = "band68",
                  "APCP" = "band69",
                  "DSWRF" = "band78",
                  "DLWRF" = "band79") |>
    dplyr::mutate(datetime = lubridate::as_datetime(time)) |>
    dplyr::select(-FID) |>
    tidyr::pivot_longer(c("PRES", "TMP", "RH", "UGRD",
                          "VGRD", "APCP", "DSWRF", "DLWRF"),
                        names_to = "variable", values_to = "prediction") |>
    dplyr::mutate(reference_datetime = lubridate::as_date(date),
                  horizon = datetime - lubridate::as_datetime(date))
  df
}
