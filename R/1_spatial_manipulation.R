download_census_tracts <- function() {
  census_tracts <- geobr::read_census_tract(
    "all",
    year = 2010,
    simplified = FALSE,
    showProgress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  census_tracts <- sf::st_make_valid(census_tracts)

  return(census_tracts)
}

download_statistical_grid <- function() {
  statistical_grid <- geobr::read_statistical_grid(
    "all",
    year = 2010,
    showProgress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  statistical_grid <- statistical_grid[
    ,
    c("ID_UNICO", "MASC", "FEM", "POP", "DOM_OCU", "geom")
  ]
  
  return(statistical_grid)
}

download_urban_concentrations <- function() {
  urban_concentrations <- geobr::read_urban_concentrations(simplified = FALSE)
  urban_concentrations <- sf::st_make_valid(urban_concentrations)
  urban_concentrations <- dplyr::summarize(
    urban_concentrations,
    geom = sf::st_union(geom),
    .by = c(code_urban_concentration, name_urban_concentration)
  )
  
  return(urban_concentrations)
}

# census_tracts <- tar_read(census_tracts)
# urban_concentrations <- tar_read(urban_concentrations)
subset_urban_conc_tracts <- function(census_tracts, urban_concentrations) {
  # usign a slightly larger buffer just to make sure that all census tracts that
  # intersect with a grid cell are included, otherwise we would lose information
  # when reaggregating data from the tracts to the grid
  intersections <- sf::st_intersects(
    census_tracts,
    sf::st_buffer(urban_concentrations, 3000) 
  )
  does_intersect <- lengths(intersections) > 0
  
  filtered_tracts <- census_tracts[does_intersect, ]
  
  return(filtered_tracts)
}