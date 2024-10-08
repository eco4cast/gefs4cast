% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/neon_sites.R
\name{neon_sites}
\alias{neon_sites}
\title{NEON sites as a simple features point geometry object}
\usage{
neon_sites(crs = sf::st_crs(grib_wkt()))
}
\arguments{
\item{crs}{coordinate reference system to be used for the desired return}
}
\value{
an sf object with coordinates for NEON sites
}
\description{
NEON sites as a simple features point geometry object
}
\examples{
\donttest{
sites <- neon_sites()
}
}
