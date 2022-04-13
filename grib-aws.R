library(terra)
library(purrr)
gefs <- function(
    horizon = "000", # 000:384 hrs ahead
    base = "https://noaa-gefs-pds.s3.amazonaws.com/",
    date = "20220316",
    cycle = "00",    # 00, 06, 12, 18 hr issued
    series = "atmos",
    set = "pgrb2a", # or pgrb2b for less common vars
    NN = "p01", # p01-p20 replicates, or  "avg"
    res = "0p50" # half 0.50 degree resolution
) {
  glue::glue("/vsicurl/{base}",
             "gefs.{date}/{cycle}/{series}/{set}p5/",
             "ge{NN}.t{cycle}z.{set}.{res}.f{horizon}")
}
c("006") -> i1
paste0("0",as.character(seq(12,96,by=6))) -> i2
as.character(seq(102,840, by=6)) -> i3
src <- map_chr(c(i1,i2,i3), gefs)

gdal <- paste("gdal_translate -of GTIFF -b 63 -b 64 -b 65 -b 66 -b 67 -b 68 -b 69 -b 70", src, paste0(basename(src), ".tif &"))
gdal <- c(gdal, "wait", "echo 'Finshed!'")

readr::write_lines(gdal, "src.sh")

bench::bench_time(system2("bash", "src.sh"))
