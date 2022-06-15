library(neonstore)
neon_download(product = "DP1.00006.001", table = "THRPRE_30min-basic") # Precip, thoughfall
neon_download(product = "DP1.00098.001", table = "RH_30min") # Humidity, note two different sensor positions
neon_download(product = "DP1.00003.001", table= "TAAT_30min") # Temp (triple-aspirated)
neon_download(product = "DP1.00002.001", table="SAAT_30min-basic") #Temp single aspirated
neon_download(product = "DP1.00023.001", table = "SLRNR_30min-basic") # Short and long wave radiation
neon_download(product = "DP1.00006.001", table = "SECPRE_30min-basic") # Precipitation secondary

neon_store(product = "DP1.00006.001", table = "THRPRE_30min-basic") # Precip, thoughfall
neon_store(product = "DP1.00098.001", table = "RH_30min") # Humidity, note two different sensor positions
neon_store(product = "DP1.00003.001", table= "TAAT_30min") # Temp (triple-aspirated)
neon_store(product = "DP1.00002.001", table="SAAT_30min-basic") #Temp single aspirated
neon_store(product = "DP1.00023.001", table = "SLRNR_30min-basic") # Short and long wave radiation
neon_store(product = "DP1.00006.001", table = "SECPRE_30min-basic") # Precipitation secondary





library(neonstore)
library(dplyr)
#db <- neon_db()
#DBI::dbListTables(db)

precip <- neon_table(table = "THRPRE_30min-basic", lazy=TRUE) # Precip, thoughfall
rh <- neon_table(table = "RH_30min", lazy=TRUE) # Humidity, note two different sensor positions
temp <- neon_table(table= "TAAT_30min", lazy=TRUE) # Temp (triple-aspirated)
slr <- neon_table(table = "SLRNR_30min-basic", lazy=TRUE) # Short and long wave radiation
# temp_s <- neon_table(table="SAAT_30min-basic", lazy=TRUE) #Temp single aspirated
#precip2 <- neon_table(table = "SECPRE_30min-basic", lazy=TRUE) # Precipitation secondary


obs_rh <- rh |> 
  filter(startDateTime >= as.Date("2022-04-20")) |>
  select(startDateTime, siteID, RHMean, horizontalPosition, verticalPosition) |>
  group_by(siteID, startDateTime) |> 
  summarise(observed = mean(RHMean)) |>
  mutate(variable = "RH") |>
  rename(site_id = siteID, time = startDateTime) |> 
  arrange(site_id,time)




endpoint <-  "js2.jetstream-cloud.org:8001"
s3 <- arrow::s3_bucket("drivers", endpoint_override =  endpoint)
s3$ls("noaa/neon/gefs")
path <- s3$path("noaa/neon/gefs")
df <- arrow::open_dataset(path)

fc <- df |> filter(start_time >= as.Date("2022-04-20"),
             variable == "RH") |> 
  collect()
target <- collect(obs_rh)

library(score4cast)
scores <- crps_logs_score(fc, target)

scores |> filter(!is.na(crps))
