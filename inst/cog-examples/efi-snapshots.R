
# Stage 1:
# Compute only mean and std for every historic date
#
# Partition: gefs-v12/stage-stats/reference_datetime/site


# Partition: gefs/stage1/reference_datetime/site
# Schema:

library(gdalcubes)
library(parallel)
library(dplyr)
gdalcubes_options(parallel=TRUE)

devtools::load_all()
library(arrow)

sf_sites = neon_sites()

## Cycles & Horizons:
# cycles 00, all horizons.
# stage3 cycles 00, 06, 12, 18: 00, 03, 06
# pass named vector of bands


# can start back to 2017-01-01
bench::bench_time({
gefs_to_parquet(dates = Sys.Date()-1, # seq(as.Date("2023-01-01"), Sys.Date()-1, by=1),
                ensemble = c(mean = "geavg", spr = "gespr"),
                gefs_s3_dir("stage1-stats"), sf_sites = neon_sites())
})

gefs_to_parquet(dates = seq(as.Date("2023-01-01"), Sys.Date()-1, by=1),
                ensemble = c("gec00", paste0("gep", stringr::str_pad(1:30, 2, pad="0"))),
                gefs_s3_dir("stage1"), sf_sites = neon_sites())

