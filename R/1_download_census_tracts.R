download_census_tracts <- function() {
  census_tracts <- geobr::read_census_tract(
    "all",
    year = 2010,
    simplified = FALSE,
    showProgress = TRUE
  )
  
  return(census_tracts)
}