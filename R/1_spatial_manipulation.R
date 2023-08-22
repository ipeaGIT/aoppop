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

# census_statistical_grid <- tar_read(census_statistical_grid)
# urban_concentrations <- tar_read(urban_concentrations)
subset_urban_conc_grid <- function(census_statistical_grid,
                                   urban_concentrations) {
  intersections <- sf::st_intersects(
    census_statistical_grid,
    sf::st_buffer(urban_concentrations, 1000)
  )
  does_intersect <- lengths(intersections) > 0
  
  filtered_grid <- census_statistical_grid[does_intersect, ]
  
  return(filtered_grid)
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
  filtered_tracts <- sf::st_make_valid(filtered_tracts)
  filtered_tracts <- sf::st_buffer(filtered_tracts, 0)
  
  # some tracts overlap, which create problems when later reaggregating census
  # data to hex grid (population count would be overcounted)
  # FIXME: worry about this warning?
  #   - although coordinates are longitude/latitude, st_difference assumes that
  #     they are planar
  
  filtered_tracts <- sf::st_difference(filtered_tracts)
  
  return(filtered_tracts)
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