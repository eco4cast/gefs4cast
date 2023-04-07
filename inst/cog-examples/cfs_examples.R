

devtools::load_all()

options("mc.cores"=parallel::detectCores())
gdalcubes_cloud_config()

bench::bench_time({
  cfs_stars(Sys.Date()-1,  horizon = cfs_horizon)
})


ens <- 1
reference_datetime <- as.Date("2023-03-02")
#sites <- neon_sites()
sites <- neon_sites() |> sf::st_shift_longitude()
horizon = cfs_horizon(ens, reference_datetime)
h <- horizon[[2]]

h <- gefs_horizon()[[2]]
gefs_urls("geavg", reference_datetime, horizon=h)
cfs_url(ens = ens,
        reference_datetime = reference_datetime,
        horizon = h)

options("mc.cores"=parallel::detectCores())
bench::bench_time({ # about 7min on all cores, needs ~ 50 GB, or 4 min per ens in 10 GB

  df <- stars_extract(ens = 1,
                      reference_datetime = reference_datetime,
                      sites=sites,
                      horizon = cfs_horizon,
                      bands = cfs_band_numbers(),
                      url_builder = cfs_urls,
                      cycle = "00")

})

lobstr::obj_size(df)

grib <- cfs_grib_collection(date = reference_datetime, ens=1)
