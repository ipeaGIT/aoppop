# tracts_with_data <- tar_read(tracts_with_data)
# urban_conc <- tar_read(individual_urban_concentrations)[1, ]
filter_tracts_with_data <- function(tracts_with_data, urban_conc) {
  tracts_with_data <- sf::st_sf(tracts_with_data)
  
  urban_conc <- sf::st_transform(urban_conc, sf::st_crs(tracts_with_data))
  
  intersections <- sf::st_intersects(
    tracts_with_data,
    sf::st_buffer(urban_conc, 2000)
  )
  do_intersect <- lengths(intersections) > 0
  
  individual_tracts <- tracts_with_data[do_intersect, ]
  
  individual_tracts_or_error <- tryCatch(
    sf::st_difference(individual_tracts),
    error = function(cnd) cnd
  )
  
  if (inherits(individual_tracts_or_error, "error")) {
    individual_tracts <- sf::st_buffer(individual_tracts, 0)
    
    individual_tracts_or_error <- tryCatch(
      sf::st_difference(individual_tracts),
      error = function(cnd) cnd
    )
  } else {
    return(individual_tracts_or_error)
  }
  
  return(individual_tracts)
}

# stat_grid <- tar_read(statistical_grid_with_pop)
# tracts_with_data <- tar_read(individual_tracts_with_data)[[1]]
filter_individual_stat_grids <- function(stat_grid, tracts_with_data) {
  # we first subset the statistical grid using st_intersects() because it is way
  # faster than st_intersection() (just using the latter would yield the same
  # result, but would take much longer)
  
  unified_tracts <- sf::st_union(tracts_with_data)
  unified_tracts <- sf::st_make_valid(unified_tracts)
  
  intersections <- sf::st_intersects(stat_grid, unified_tracts)
  do_intersect <- lengths(intersections) > 0
  
  filtered_grid <- stat_grid[do_intersect, ]
  
  # finally, we have to intersect the statistical grid with the census tracts
  # that cover the urban concentrations, otherwise we would not properly count
  # the total population in each intersection between grid cells and tracts when
  # aggregating census data to the statistical grid. we use the 'grid_with_data'
  # object to make sure that we're not considering tracts that don't have any
  # population
  # suppressed warning: 
  #  - attribute variables are assumed to be spatially constant throughout all
  #    geometries
  
  suppressWarnings(
    filtered_grid <- sf::st_intersection(filtered_grid, unified_tracts)
  )
  
  return(filtered_grid)
}