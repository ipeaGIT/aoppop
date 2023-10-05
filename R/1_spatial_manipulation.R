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

download_pop_arrangements <- function() {
  pop_arrangements <- geobr::read_pop_arrangements(simplified = FALSE)
  pop_arrangements <- sf::st_make_valid(pop_arrangements)
  pop_arrangements <- dplyr::summarize(
    pop_arrangements,
    geom = sf::st_union(geom),
    population = sum(pop_total_2010),
    .by = c(code_pop_arrangement, name_pop_arrangement)
  )
  
  return(pop_arrangements)
}

# concs <- tar_read(urban_concentrations)
# arrangs <- tar_read(pop_arrangements)
merge_pop_units <- function(concs, arrangs) {
  # a population arrangement is a group of 2+ cities in which there is a strong
  # integration of population groups, either due to commuting flows that cross
  # different cities or due to the contiguity of their urban areas. these
  # population arrangements are defined by IBGE.
  #
  # urban concentrations are population arrangements or individual cities with
  # more than 100k residents.
  #
  # not all population arrangements are urban concentrations (those that have
  # less than 100k residents), and not all urban concentrations are population
  # arrangements (those that are composed of only one city).
  #
  # here, we call "population units" (pop_units) the union of all population
  # arrangements and urban concentrations.
  #
  # ps: the population arrangements of Ponta Porã (code 5006606) and Sant'Ana do
  # Livramento (4317103) have less than 100k residents in the dataset, but they
  # have more than 100k when accounting the residents of the cities in other
  # countries (in Paraguai and Uruguai, respectively), so they are actually 
  # classified as urban concentrations. we manually remove them from
  # 'small_arrangs' below
  
  all_concs <- concs
  names(all_concs) <- c("code_pop_unit", "name_pop_unit", "geom")
  
  small_arrangs <- arrangs[arrangs$population < 100000, ]
  small_arrangs <- dplyr::select(
    small_arrangs,
    code_pop_unit = code_pop_arrangement,
    name_pop_unit = name_pop_arrangement
  )
  small_arrangs <- subset(
    small_arrangs,
    ! code_pop_unit %in% c(5006606, 4317103)
  )
  
  pop_units <- rbind(all_concs, small_arrangs)
  
  pop_units <- pop_units[order(pop_units$code_pop_unit), ]
  pop_units$tar_group <- 1:nrow(pop_units)
  
  return(pop_units)
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

# res <- tar_read(h3_resolutions)[1]
# urban_concentration <- tar_read(individual_urban_concentrations)[1, ]
create_hex_grids <- function(res, urban_concentration) {
  urban_conc_cells <- h3jsr::polygon_to_cells(urban_concentration, res)
  urban_conc_grid <- h3jsr::cell_to_polygon(urban_conc_cells, simple = FALSE)
  
  # some adjacent urban concentrations (Rio de Janeiro and Angra, for example)
  # may end up sharing some cells is common. in order to prevent the same cell
  # from appearing twice in our final output, we keep only cells whose centroids
  # are within the polygon.
  # suppressed warning:
  #  - st_centroid assumes attributes are constant over geometries
  
  grid_centroids <- suppressWarnings(sf::st_centroid(urban_conc_grid))
  
  contained_cells <- sf::st_within(grid_centroids, urban_concentration)
  within_concentration <- lengths(contained_cells) > 0
  
  urban_conc_grid <- urban_conc_grid[within_concentration, ]
  
  return(urban_conc_grid)
}


# res <- tar_read(h3_resolutions)[1]
# pop_unit <- tar_read(pop_units)[1, ]
create_hex_grid <- function(res, pop_unit) {
  original_crs <- sf::st_crs(pop_unit)
  
  pop_unit_wgs <- sf::st_transform(pop_unit, 4326)
  pop_urban_cells <- h3jsr::polygon_to_cells(pop_unit_wgs, res)
  
  pop_urban_grid <- h3jsr::cell_to_polygon(pop_urban_cells, simple = FALSE)
  pop_urban_grid <- sf::st_transform(pop_urban_grid, original_crs)
  
  return(pop_urban_grid)
}
