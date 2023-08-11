suppressPackageStartupMessages({
  library(targets)
})

source("R/1_download_census_tracts.R", encoding = "UTF-8")

list(
  tar_target(census_tracts, download_census_tracts())
)
