
# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs/mean/reference_datetime/site


# Partition: gefs/stage1/reference_datetime/site
# Schema:

library(gefs4cast)
library(gdalcubes)
library(parallel)
gdalcubes_options(parallel=TRUE)

sf_sites <- neon_sites()

date <- as.Date("2023-02-20")
bench::bench_time({ # 1.39 hrs for 30 ensemble members
  view <- gefs_cube_view(date)
  #ensemble <-  paste0("gep", stringr::str_pad(1:30, 2, pad="0"))
  ensemble = c("geavg", "gespr") # mean and spread
  dfs <- lapply(ensemble, grib_extract, date = date, sites = sf_sites)
})


df <-
  purrr::list_rbind(dfs, names_to = "statistic") |>
  tibble::as_tibble() |>
  dplyr::inner_join(df, sf_sites) |>
  dplyr::rename("PRES"= band57,
                "TMP" = band63,
                "RH" = band64,
                "UGRD" = band67,
                "VGRD" = band68,
                "APCP" = band69,
                "DSWRF" = band78,
                "DLWRF" = band79) |>
  pivot_longer(c("PRES", "TMP", "RH", "UGRD", "VGRD", "APCP", "DSWRF", "DLWRF"),
               names_to = "variable", values_to = "prediction")





