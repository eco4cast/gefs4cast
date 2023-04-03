

devtools::load_all()

ens <- 1
reference_datetime <- as.Date("2023-03-02")
options("mc.cores"=parallel::detectCores())
bands <- cfs_bands()
sites <- neon_sites() |> sf::st_shift_longitude()

bench::bench_time({
  df <-
    cfs_grib_collection(ens, reference_datetime) |>
    gdalcubes::select_bands(bands) |>
    gdalcubes::extract_geom(sites)

})

dfs <- parallel::mclapply(ensemble,
                          grib_extract,
                          date = date,
                          sites = sites,
                          bands = bands,
                          cycle = cycle,
                          horizon = horizon,
                          mc.cores = getOption("mc.cores", 1L))
dfs |>
  efi_format_cubeextract(date = date, sites = sites) |>
  dplyr::mutate(family = family)






bench::bench_time({ # about 7min on all cores, needs ~ 50 GB, or 4 min per ens in 10 GB
  parallel::mclapply(1:4, function(ens) {
  df <- cfs_stars_extract(ens, reference_datetime)
  })
})

lobstr::obj_size(df)

grib <- cfs_grib_collection(date = reference_datetime, ens=1)
