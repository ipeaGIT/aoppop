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
