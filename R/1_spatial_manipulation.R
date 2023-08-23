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
  # FIXME: figure out why the st_difference() call results in an error here
  # filtered_tracts <- sf::st_make_valid(filtered_tracts)
  # filtered_tracts <- sf::st_buffer(filtered_tracts, 0)
  
  # # some tracts overlap, which create problems when later reaggregating census
  # # data to hex grid (population count would be overcounted)
  # # FIXME: worry about this warning?
  # #   - although coordinates are longitude/latitude, st_difference assumes that
  # #     they are planar
  # 
  # filtered_tracts <- sf::st_difference(filtered_tracts)
  
  return(filtered_tracts)
}

# stat_grid <- tar_read(census_statistical_grid)
# urban_concentrations <- tar_read(urban_concentrations)
# tracts_with_data <- tar_read(tracts_with_data)
subset_urban_conc_grid <- function(stat_grid,
                                   urban_concentrations,
                                   tracts_with_data) {
  # we can remove grid cells that don't have any population on them, because
  # they will not affect the aggregation processes from the census tracts to the
  # statistical grid and from the grid to the hexagons
  
  stat_grid <- stat_grid[stat_grid$POP > 0, ]
  
  # creating a small buffer around the urban concentrations helps us make sure
  # that our statistical grid will fully cover the hexagon grid that we will use
  # (the hexagons can extend beyond the urban concentrations' limits, due to
  # their shape)
  
  intersections <- sf::st_intersects(
    stat_grid,
    sf::st_buffer(urban_concentrations, 1000)
  )
  does_intersect <- lengths(intersections) > 0
  
  filtered_grid <- stat_grid[does_intersect, ]
  
  # finally, we have to intersect the statistical grid with the census tracts
  # that cover the urban concentrations, otherwise we would not properly count
  # the total population in each intersection between grid cells and tracts when
  # aggregating census data to the statistical grid. we use the 'grid_with_data'
  # object to make sure that we're not considering tracts that don't have any
  # population
  # (unifying the tracts before intersecting speeds up the process) 
  
  n_cores <- min(8, getOption("TARGETS_N_CORES"))
  
  tracts_with_data <- sf::st_sf(tracts_with_data)
  unified_tracts <- parallel_union(tracts_with_data, n_cores)
  
  filtered_grid <- parallel_intersection(
    filtered_grid,
    unified_tracts,
    n_cores
  )
  
  return(filtered_grid)
}

# urban_concentrations <- tar_read(urban_concentrations)
# res <- tar_read(h3_resolutions)[3]
create_hex_grid <- function(urban_concentrations, res) {
  urban_concentrations <- sf::st_transform(urban_concentrations, 4326)
  
  urban_conc_cells <- h3jsr::polygon_to_cells(urban_concentrations, res = res)
  
  if (res == 9) {
    # when using res 9, if we don't split the cells' list before converting it
    # to polygon we run into a "javascript heap out of memory error". after some
    # visual inspection, it seems like splitting the cells into 15 batches and
    # using 15 cores to do this was good enough and didn't make too many
    # processes idle
    
    n_batches <- 15
    
    indices_cuts <- cut(
      1:length(urban_conc_cells),
      breaks = n_batches
    )
    batch_indices <- split(1:length(urban_conc_cells), indices_cuts)
    
    future::plan(future::multisession, workers = n_batches)
    
    urban_conc_grid_list <- furrr::future_map(
      batch_indices,
      function(is) h3jsr::cell_to_polygon(urban_conc_cells[is], simple = FALSE),
      .options = furrr::furrr_options(seed = TRUE)
    )
    
    future::plan(future::sequential)
    
    urban_conc_grid <- data.table::rbindlist(urban_conc_grid_list)
    
    # there are some (very few) duplicate entries, because the same hexagon may
    # intersect with more than one urban concentration. so we have to keep only
    # unique entries
    
    urban_conc_grid <- urban_conc_grid[
      urban_conc_grid[, .I[1], by = h3_address]$V1
    ]
    urban_conc_grid <- sf::st_as_sf(urban_conc_grid)
  } else {
    urban_conc_grid <- h3jsr::cell_to_polygon(urban_conc_cells, simple = FALSE)
  }
  
  return(urban_conc_grid)
}

parallel_union <- function(object, n_cores) {
  if (n_cores == 1) {
    batch_indices = list(1:nrow(object))
  } else {
    indices_cuts <- cut(1:nrow(object), breaks = n_cores)
    batch_indices <- split(1:nrow(object), indices_cuts)
  }
  
  future::plan(future::multisession, workers = n_cores)
  
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
  
  future::plan(future::sequential)
  
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
  if (n_cores == 1) {
    batch_indices = list(1:nrow(x))
  } else {
    indices_cuts <- cut(1:nrow(x), breaks = n_cores)
    batch_indices <- split(1:nrow(x), indices_cuts)
  }
  
  future::plan(future::multisession, workers = n_cores)
  
  intersections_list <- furrr::future_map(
    batch_indices,
    function(is) sf::st_intersection(x[is, ], y),
    .options = furrr::furrr_options(seed = TRUE, globals = c("x", "y")),
    .progress = getOption("TARGETS_SHOW_PROGRESS")
  )
  
  future::plan(future::sequential)
  
  # the class of the geom column of each list element may be different,
  # depending whether the intersection result in polygon or multipolygon
  # objects. so we check if they are different, and, if so, we cast it to the
  # same class
  
  intersected_obj <- do.call(rbind, intersections_list)
  
  return(intersected_obj)
}