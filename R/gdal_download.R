
# For info mapping band id numbers to descriptions, see 
# https://www.nco.ncep.noaa.gov/pmb/products/gens/gep01.t00z.pgrb2a.0p50.f003.shtml
# could easily generalize this to take compression and output format as options
gdal_download <- function(src, 
                          vars,
                          dest = ".", 
                          threads=70, 
                          gdal_ops = "-co compress=zstd"
) {
  
  GDAL_BIN <- Sys.getenv('GDAL_BIN', "")
  
  assert_gdal()
  gdal <- paste(paste0(GDAL_BIN,
                       "gdal_translate"), 
                gdal_ops,
                "-of GTIFF",
                vars,
                src, 
                file.path(dest, paste0(basename(src), ".tif &")))
  groups <- c(seq(1, length(src), by=threads), length(src))
  cmd <- c("#!/bin/bash", "set -e")
  for(i in 1:(length(groups)-1)){
    cmd <- c(cmd, gdal[seq(groups[i],groups[i+1], by=1)], "wait")
  }
  shell <- "src.sh"
  cmd <- c(cmd, "wait", "echo 'Finshed!'")
  readr::write_lines(cmd, shell)
  p <- processx::run("bash", shell)
  
  unlink(shell)
  invisible(p)
}  


assert_gdal <- function() {
  GDAL_BIN <- Sys.getenv('GDAL_BIN', "")
  
  x <- processx::run(paste0(GDAL_BIN, "gdalinfo"), "--version")
  version <- gsub("GDAL (\\d\\.\\d\\.\\d), .*", "\\1", x$stdout)
  stopifnot(utils::compareVersion(version, "3.4.0") >=0 )
}

