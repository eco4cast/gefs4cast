# Access NOAA's Global Ensemble Forecast System Archive on AWS

<!-- badges: start -->
[![R-CMD-check](https://github.com/eco4cast/gefs4cast/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/eco4cast/gefs4cast/actions/workflows/R-CMD-check.yaml)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

Cloud-native access and downloading of NOAA's GEFS data. **Caution** package is highly experimental and not yet suitable for general use.  Direct access to EFI-processed GEFS forecasts for NEON sites may be available through the [`neon4cast` package](https://github.com/eco4cast/neon4cast).

Example scripts can be seen in the `inst/examples` directory.

For variable definitions, see the [NOAA tables](https://www.nco.ncep.noaa.gov/pmb/products/gens/gec00.t00z.pgrb2a.0p50.f000.shtml) (If selecting band numbers from the grib files, note band numbers differ between the 000 hour and > 003 forecast)
