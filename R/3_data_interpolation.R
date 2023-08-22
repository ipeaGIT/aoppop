# urban_concentration_tracts <- tar_read(urban_concentration_tracts)
# processed_census_data <- tar_read(processed_census_data)
merge_census_tracts_data <- function(urban_concentration_tracts,
                                     processed_census_data) {
  data.table::setDT(urban_concentration_tracts)
  
  tracts_with_data <- merge(
    urban_concentration_tracts,
    processed_census_data,
    by.x = "code_tract",
    by.y = "cod_setor"
  )
  
  return(tracts_with_data)
}

# tracts_with_data <- tar_read(tracts_with_data)
# stat_grid <- tar_read(urban_concentration_stat_grid)
agg_census_data_to_grid <- function(tracts_with_data, stat_grid) {
  # we can remove grid cells that don't have any population on them, because the
  # rest of the variables will equally result in 0 because of it. therefore,
  # they will not affect the "final reaggregation", in which we aggregate data
  # from the grid cells to the hexagons
  
  stat_grid <- stat_grid[stat_grid$POP > 0, ]
  
  # we have to intersect the statistical grid with the tracts, so we can make
  # sure that there's a perfect spatial correspondence between the grid cells
  # and the tracts (otherwise we would not properly count the total population
  # in each intersection between grid cells and tracts)
  
  tracts_with_data <- sf::st_sf(tracts_with_data)
  
  unified_tracts <- parallel_union(
    tracts_with_data,
    n_cores = min(10, getOption("TARGETS_N_CORES"))
  )
  
  grid_tracts_intersection <- parallel_intersection(
    stat_grid,
    unified_tracts,
    n_cores = min(30, getOption("TARGETS_N_CORES"))
  )
}