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
  cols_to_drop <- c(
    "zone", "code_muni", "name_muni", "name_neighborhood", "code_neighborhood",
    "code_subdistrict", "name_subdistrict", "code_district", "name_district",
    "code_state", "cod_uf", "cod_muni"
  )
  tracts_with_data[, (cols_to_drop) := NULL]
  
  tracts_with_data <- sf::st_sf(tracts_with_data)
  tracts_with_data$tract_total_area <- as.numeric(sf::st_area(tracts_with_data))
  
  stat_grid$grid_total_area <- as.numeric(sf::st_area(stat_grid))
  
  grid_tracts_intersection <- parallel_intersection_in_batches(
    stat_grid[1:30, ],
    tracts_with_data,
    n_batches = 1,
    n_cores = min(2, getOption("TARGETS_N_CORES"))
  )
  
  grid_tracts_intersection$intersect_area <- as.numeric(
    sf::st_area(grid_tracts_intersection)
  )
}

parallel_intersection_in_batches <- function(x, y, n_batches, n_cores) {
  if (n_batches == 1) {
    batch_indices = list(1:nrow(x))
  } else {
    indices_cuts <- cut(1:nrow(x), breaks = n_batches)
    batch_indices <- split(1:nrow(x), indices_cuts)
  }
  
  intersections_list <- lapply(
    batch_indices,
    function(is) {
      unified_x <- parallel_union(x[is, ], n_cores)
      
      intersecting_y <- sf::st_intersects(y, unified_x)
      filtered_y <- y[lengths(intersecting_y) > 0, ]
      
      parallel_intersection(x[is, ], filtered_y, n_cores)
    }
  )
  
  intersected_obj <- do.call(rbind, intersections_list)
  intersected_obj <- sf::st_make_valid(intersected_obj)
  
  return(intersected_obj)
}