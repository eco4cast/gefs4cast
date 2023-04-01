


ens <- 1

reference_datetime <- as.Date("2023-03-02")
options("mc.cores"=parallel::detectCores())
bench::bench_time({ # about 7min on all cores, needs ~ 50 GB, or 4 min per ens in 10 GB
  parallel::mclapply(1:4, function(ens) {
  df <- cfs_stars_extract(ens, reference_datetime)
  })
})

lobstr::obj_size(df)

grib <- cfs_grib_collection(date = reference_datetime, ens=1)
