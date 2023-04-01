

library(vis4cast)
vis4cast::ignore_sigpipe()

devtools::load_all()

gdalcubes::gdalcubes_options(parallel=TRUE)
options("mc.cores"=1L) # 11min w/ 31 cores, 5.32 hrs on 1 core.  on top of gdalcubes parallel use of of all cores


stage1 <- gefs4cast::gefs_s3_dir("stage1")
dates <- seq(as.Date("2020-10-01"), Sys.Date() -1, by=1) |> as.character()
who <- stage1$ls()

has <- stringr::str_extract(who, "\\d{4}-\\d{2}-\\d{2}")
missing <- dates[ !(dates %in% has) ]


dates <- missing
sites = neon_sites()


bench::bench_time({
  gefs_to_parquet(dates,
                  sites = sites,
                  path = stage1)
})

