library(tidyverse)
library(processx)
library(glue)
library(terra)
source("R/gefs.R")
source("R/neon.R")

# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
dates <- seq(Sys.Date(), Sys.Date()-3, length.out=33)
map(dates, noaa_gefs, cycle=00, threads=70, endpoint = "data.ecoforecast.org")