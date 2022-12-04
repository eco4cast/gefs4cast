

gefs_url <- function(
    horizon = "000", # 000:384 hrs ahead
    date = "20220314",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "gep01", # p01-p20 replicates, or  "avg"
    res = "0p50", # half 0.50 degree resolution
    base = "https://noaa-gefs-pds.s3.amazonaws.com/"
) {
  glue::glue("/vsicurl/{base}",
             "gefs.{date}/{cycle}/{series}/{set}p5/",
             gefs_filename(horizon, cycle, set, NN, res))
}

gefs_filename <- function(horizon, cycle = "00", set = "pgrb2a", NN = "gep01",
                          res = "0p50",   extension = "") {
  glue::glue("{NN}.t{cycle}z.{set}.{res}.f{horizon}{extension}")
}
