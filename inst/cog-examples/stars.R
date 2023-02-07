
tifs <- c("gep01.t00z.pgrb2a.0p50.f003.tif", "gep01.t00z.pgrb2a.0p50.f006.tif")
urls <- paste0("/vsicurl/",
"https://sdsc.osn.xsede.org/bio230014-bucket01/neon4cast-drivers/",
"noaa/gefs-v12/cogs/gefs.20221201/", tifs
)

library(stars)
#stars::read_stars(urls, along="time") # no luck!


## grab unstacked
x <- lapply(urls, read_stars)

# extract band-names-part
band_names <- st_get_dimension_values(x[[1]], "band") |> 
  stringr::str_extract("([A-Z]+):") |>
  str_remove(":")
# apply corrected band-names
x1 <- lapply(x, st_set_dimensions, "band", band_names)

## Life would be easier if we corrected these: tada!
# purrr::walk2(x1, tifs, write_stars)
# z = stars::read_stars(tifs, along=list("time"=dates))


# at last, we can stack into a cube:
x1 <- do.call(c, c(x1, along="time"))

# and add correct date timestamps to the new time dimension
dates <- as.Date("2022-12-01") + lubridate::hours(c(3,6))
x1 <- st_set_dimensions(x1, "time", dates)
x1

