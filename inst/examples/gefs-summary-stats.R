Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

s3 <- arrow::s3_bucket("drivers/noaa/neon/gefs", 
                       endpoint_override =  "data.ecoforecast.org",
                       anonymous=TRUE)
df2 <- arrow::open_dataset(s3, partitioning = c("start_date", "offset"))
df <- arrow::open_dataset(s3)

library(dplyr)
tmp2 <- df2 |> 
  filter(ensemble == 1, horizon==0, site_id == "BART", variable == "TMP") |> 
  collect() 


tmp2 |>  count(offset)
tmp2 |> distinct(start_date, offset) |> count(start_date) |> filter(n<4) |> pull(start_date) -> missing
dates <- as.Date(missing)


tmp2 |> distinct(offset) |> filter(start_date %in% missing) |> count(offset)



tmp |> filter(start_date == as.Date("2022-01-05"))

# temporal coverage
df |> select(start_time) |> distinct() |> collect() |> pull(start_time) -> times
min(times)
max(times)
length(times)

dates <- lubridate::as_date(times)
x <- table(dates)
missing <- names(x[x!=4]) # less than four
dates <- missing

hour <- lubridate::hour(times)
# whoops not at all
df |> summarise(min = min(start_time), max = max(start_time)) |> collect()













