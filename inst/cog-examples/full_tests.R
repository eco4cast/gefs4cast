library(testthat)

devtools::load_all()
library(gdalcubes)
library(arrow)
# c6in.4xlarge:
# cirrus: 5 min all 128 cores 2x overload or 2

test_that("gdalcubes-based CFS", {

  path <- tempfile()
  gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
  bench::bench_time({
    cfs_to_parquet(Sys.Date()-3, path = path)
  })
  df <- arrow::open_dataset(path)
})

test_that("stars-based CFS", {

  path = tempfile()
  options("mc.cores"=parallel::detectCores())
  bench::bench_time({
    cfs_stars(Sys.Date()-1, path)
  })
  df <- arrow::open_dataset(path)

})


test_that("gdalcubes-based GEFS", {

  path <- tempfile()
  gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
  bench::bench_time({
    gefs_to_parquet(Sys.Date()-3, path = path)
  })
  df <- arrow::open_dataset(path)
})

test_that("stars-based GEFS", {

  path = tempfile()
  options("mc.cores"=parallel::detectCores())
  bench::bench_time({
    gefs_stars(Sys.Date()-1, path)
  })
  df <- arrow::open_dataset(path)

})

## Access data
test_that("We can accessed the published  ensemble forecast", {
  library(dplyr)
  library(ggplot2)
  library(arrow)
  s3 <- cfs_s3_dir("6hrly/00")
  cfs_data <- arrow::open_dataset(s3$path("reference_datetime=2021-02-15"))
  df <- cfs_data |> collect()

  df |> filter(site_id == "ABBY", variable=="TMP_srf") |>
    ggplot(aes(datetime, prediction, col=parameter)) + geom_line()

})
