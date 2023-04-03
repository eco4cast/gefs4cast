
  library(gdalcubes)
  gdalcubes_options(parallel=TRUE)
  devtools::load_all()

  stage1 <- gefs_s3_dir("stage1")
  sites <- neon_sites()
  dates <- seq(as.Date("2022-01-01"), as.Date("2023-01-01"), by=1)

  options("mc.cores"=8)
  bench::bench_time({
      gefs_to_parquet(dates, path=stage1, sites=sites)
  })

stage1_stats <- gefs_s3_dir("stage1-stats")
bench::bench_time({
  gefs_to_parquet(dates,
                  path=stage1,
                  ensemble = c(mean = "geavg", spr = "gespr"),
                  sites=sites)
})
