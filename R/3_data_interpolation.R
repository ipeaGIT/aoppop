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
  tracts_with_data <- sf::st_sf(tracts_with_data)
  
  # we have to intersect the statistical grid with the tracts, so we can make
  # sure that there's a perfect spatial correspondence between the grid cells
  # and the tracts (otherwise we would not properly count the total population
  # in each intersection between grid cells and tracts)
  
  n_cores <- min(15, getOption("TARGETS_N_CORES"))
  future::plan(future::multisession, workers = n_cores)
  
  unified_tracts <- parallel_union(tracts_with_data, n_cores = n_cores)
  
  future::plan(future::sequential)
  future::plan(future::multisession, workers = n_cores)
  
  grid_tracts_intersection1 <- parallel_intersection(
    stat_grid[seq.int(1, as.integer(nrow(stat_grid) / 4)), ],
    tracts_with_data,
    n_cores = n_cores
  )
  
  future::plan(future::sequential)
}

parallel_union <- function(object, n_cores) {
  indices_cuts <- cut(
    1:nrow(object),
    breaks = n_cores
  )
  batch_indices <- split(1:nrow(object), indices_cuts)
  
  unified_object_list <- furrr::future_map(
    batch_indices,
    function(is) {
      x <- sf::st_union(object[is, ])
      x <- sf::st_make_valid(x)
      x
    },
    .options = furrr::furrr_options(seed = TRUE, globals = "object"),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  unified_object <- Reduce(
    function(obj1, obj2) {
      x <- sf::st_union(obj1, obj2)
      x <- sf::st_make_valid(x)
    },
    unified_object_list
  )
  
  return(unified_object)
}

parallel_intersection <- function(x, y, n_cores) {
  indices_cuts <- cut(1:nrow(x), breaks = n_cores)
  batch_indices <- split(1:nrow(x), indices_cuts)
  
  intersections_list <- furrr::future_map(
    batch_indices,
    function(is) sf::st_intersection(x[is, ], y),
    .options = furrr::furrr_options(seed = TRUE, globals = c("x", "y")),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  intersected_obj <- data.table::rbindlist(intersections_list)
  intersected_obj <- sf::st_sf(intersected_obj)
  
  return(intersected_obj)
}