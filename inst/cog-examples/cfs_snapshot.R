
devtools::load_all()
options("mc.cores"=24)

ens <- 1
reference_datetime = Sys.Date()-2

s3 <- cfs_s3_dir(product=glue::glue("6hrly"))

dates <- rev(seq(as.Date("2023-01-01"), Sys.Date()-1, by=1))

lapply(dates, function(reference_datetime){
  print(reference_datetime)
  lapply(1:4, function(ens){
    cfs_stars_extract(ens, reference_datetime) |>
      dplyr::select(-geometry) |>
      arrow::write_dataset(s3, partitioning=c("parameter", "reference_datetime"))
  })
})


