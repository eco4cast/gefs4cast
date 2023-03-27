


ens <- 1
reference_datetime <- as.Date("2022-03-02")

options("mc.cores"=parallel::detectCores())


bench::bench_time({
  df <- cfs_stars_extract(ens, reference_datetime)
})



