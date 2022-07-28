# Access NOAA's Global Ensemble Forecast System Archive on AWS

<!-- badges: start -->
[![R-CMD-check](https://github.com/eco4cast/gefs4cast/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/eco4cast/gefs4cast/actions/workflows/R-CMD-check.yaml)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

Cloud-native access and downloading of NOAA's GEFS data. **Caution** package is highly experimental and not yet suitable for general use.  Direct access to EFI-processed GEFS forecasts for NEON sites may be available through the [`neon4cast` package](https://github.com/eco4cast/neon4cast).

Example scripts can be seen in the `inst/examples` directory.

For variable definitions, see the [NOAA tables](https://www.nco.ncep.noaa.gov/pmb/products/gens/gec00.t00z.pgrb2a.0p50.f000.shtml) (If selecting band numbers from the grib files, note band numbers differ between the 000 hour and > 003 forecast)

# Accessing the data

## R 

```r
library(arrow)
library(dplyr)
Sys.unsetenv("AWS_DEFAULT_REGION")
s3 <- s3_bucket("neon4cast-drivers/noaa/gefs-v12/stage1",
                endpoint_override = "data.ecoforecast.org", 
                anonymous = TRUE)
df <- open_dataset(s3, partitioning=c("start_date", "cycle"))
df |> filter(start_date == "2022-04-02", cycle == "00", ensemble==1)

```

## Python

```python
import pyarrow.dataset as ds
from pyarrow import fs

s3 = fs.S3FileSystem(endpoint_override = "data.ecoforecast.org", anonymous = True)
dataset = ds.dataset(
    "neon4cast-drivers/noaa/gefs-v12/stage1",
    format="parquet",
    filesystem=s3,
    partitioning=["start_date", "cycle"]
)
expression = (
              (ds.field("start_date") == "2022-04-01") &
              (ds.field("cycle") == 00) &
              (ds.field("ensemble") == 1)
              )
ex = dataset.to_table(filter=expression)
```
