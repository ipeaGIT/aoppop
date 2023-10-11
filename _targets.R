options(
  TARGETS_SHOW_PROGRESS = FALSE,
  TARGETS_N_CORES = 35
)

suppressPackageStartupMessages({
  library(targets)
  library(ggplot2)
  library(sf)
})

source("R/1_spatial_manipulation.R", encoding = "UTF-8")
source("R/2_data_processing.R", encoding = "UTF-8")
source("R/3_data_interpolation.R", encoding = "UTF-8")

if (!interactive()) {
  future::plan(future.callr::callr, workers = getOption("TARGETS_N_CORES"))
}

tar_option_set(workspace_on_error = TRUE)

list(
  tar_target(h3_resolutions, 7:9),
  
  # spatial manipulation
  tar_target(census_tracts, download_census_tracts()),
  tar_target(census_statistical_grid, download_statistical_grid()),
  tar_target(urban_concentrations, download_urban_concentrations()),
  tar_target(pop_arrangements, download_pop_arrangements()),
  tar_target(
    pop_units,
    merge_pop_units(urban_concentrations, pop_arrangements),
    iteration = "group"
  ),
  tar_target(
    hex_grids,
    create_hex_grid(h3_resolutions, pop_units),
    pattern = cross(h3_resolutions, pop_units),
    format = "file",
    retrieval = "worker",
    storage = "worker",
    iteration = "list"
  ),
  tar_target(
    pop_units_tracts,
    subset_pop_units_tracts(census_tracts, pop_units)
  ),


  # data processing
  tar_target(census_data, prepare_census_data()),
  tar_target(
    tracts_with_data,
    merge_census_tracts_data(pop_units_tracts, census_data)
  ),
  tar_target(
    individual_tracts_with_data,
    filter_tracts_with_data(tracts_with_data, pop_units),
    pattern = map(pop_units),
    retrieval = "worker",
    storage = "worker",
    iteration = "list"
  ),
  tar_target(
    statistical_grid_with_pop,
    subset(census_statistical_grid, POP > 0)
  ),
  tar_target(
    individual_stat_grids,
    filter_individual_stat_grids(
      statistical_grid_with_pop,
      individual_tracts_with_data
    ),
    pattern = map(individual_tracts_with_data),
    retrieval = "worker",
    storage = "worker",
    iteration = "list"
  ),

  # data interpolation
  tar_target(
    large_stat_grids_indices,
    which(vapply(individual_stat_grids, nrow, numeric(1)) > 10000)
  ),
  tar_target(
    small_stat_grids,
    individual_stat_grids[-large_stat_grids_indices],
    iteration = "list"
  ),
  tar_target(
    large_stat_grids,
    individual_stat_grids[large_stat_grids_indices],
    iteration = "list"
  ),
  tar_target(
    small_tracts_with_data,
    individual_tracts_with_data[-large_stat_grids_indices],
    iteration = "list"
  ),
  tar_target(
    large_tracts_with_data,
    individual_tracts_with_data[large_stat_grids_indices],
    iteration = "list"
  ),
  tar_target(
    small_stat_grids_with_data,
    aggregate_data_to_stat_grid(
      small_stat_grids,
      small_tracts_with_data,
      manual_parallelization = FALSE
    ),
    pattern = map(small_stat_grids, small_tracts_with_data),
    retrieval = "worker",
    storage = "worker",
    iteration = "list"
  ),
  tar_target(
    large_stat_grids_with_data,
    aggregate_data_to_stat_grid(
      large_stat_grids,
      large_tracts_with_data,
      manual_parallelization = TRUE
    ),
    pattern = map(large_stat_grids, large_tracts_with_data),
    garbage_collection = TRUE,
    iteration = "list"
  ),
  tar_target(
    stat_grids_with_data,
    bind_stat_grids(
      large_stat_grids_with_data,
      small_stat_grids_with_data,
      large_stat_grids_indices
    ),
    iteration = "list"
  ),

  # statistical grid to hexagons res 7
  tar_target(
    large_indices_for_res_7,
    {
      large <- dplyr::filter(
        pop_units,
        code_pop_unit %in% c(3550308, 5300108, 4106902, 3304557)
      )
      indices <- large$tar_group
      indices
    }
  ),
  tar_target(
    small_stat_grids_with_data_res_7,
    stat_grids_with_data[-large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    large_stat_grids_with_data_res_7,
    stat_grids_with_data[large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    small_hex_grids_res_7,
    head(hex_grids, 376)[-large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    large_hex_grids_res_7,
    head(hex_grids, 376)[large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    small_pop_units_res_7,
    {
      units <- pop_units
      units <- units[! units$tar_group %in% large_indices_for_res_7, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    large_pop_units_res_7,
    {
      units <- pop_units
      units <- units[units$tar_group %in% large_indices_for_res_7, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    small_hexagons_with_data_res_7,
    aggregate_data_to_hexagons(
      small_pop_units_res_7,
      small_stat_grids_with_data_res_7,
      small_hex_grids_res_7,
      res = 7,
      manual_parallelization = FALSE
    ),
    pattern = map(
      small_pop_units_res_7,
      small_stat_grids_with_data_res_7,
      small_hex_grids_res_7
    ),
    retrieval = "worker",
    storage = "worker",
    iteration = "list",
    format = "file"
  ),
  tar_target(
    large_hexagons_with_data_res_7,
    aggregate_data_to_hexagons(
      large_pop_units_res_7,
      large_stat_grids_with_data_res_7,
      large_hex_grids_res_7,
      res = 7,
      manual_parallelization = TRUE
    ),
    pattern = map(
      large_pop_units_res_7,
      large_stat_grids_with_data_res_7,
      large_hex_grids_res_7
    ),
    garbage_collection = TRUE,
    iteration = "list",
    format = "file"
  ),

  # statistical grid to hexagons res 8
  tar_target(
    large_indices_for_res_8,
    which(vapply(individual_stat_grids, nrow, numeric(1)) > 4500)
  ),
  tar_target(
    small_stat_grids_with_data_res_8,
    stat_grids_with_data[-large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    large_stat_grids_with_data_res_8,
    stat_grids_with_data[large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    small_hex_grids_res_8,
    hex_grids[377:752][-large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    large_hex_grids_res_8,
    hex_grids[377:752][large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    small_pop_units_res_8,
    {
      units <- pop_units
      units <- units[! units$tar_group %in% large_indices_for_res_8, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    large_pop_units_res_8,
    {
      units <- pop_units
      units <- units[units$tar_group %in% large_indices_for_res_8, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    small_hexagons_with_data_res_8,
    aggregate_data_to_hexagons(
      small_pop_units_res_8,
      small_stat_grids_with_data_res_8,
      small_hex_grids_res_8,
      res = 8,
      manual_parallelization = FALSE
    ),
    pattern = map(
      small_pop_units_res_8,
      small_stat_grids_with_data_res_8,
      small_hex_grids_res_8
    ),
    retrieval = "worker",
    storage = "worker",
    iteration = "list",
    format = "file"
  ),
  tar_target(
    large_hexagons_with_data_res_8,
    aggregate_data_to_hexagons(
      large_pop_units_res_8,
      large_stat_grids_with_data_res_8,
      large_hex_grids_res_8,
      res = 8,
      manual_parallelization = TRUE
    ),
    pattern = map(
      large_pop_units_res_8,
      large_stat_grids_with_data_res_8,
      large_hex_grids_res_8
    ),
    garbage_collection = TRUE,
    iteration = "list",
    format = "file"
  ),

  # statistical grid to hexagons res 9
  tar_target(
    large_indices_for_res_9,
    which(vapply(individual_stat_grids, nrow, numeric(1)) > 2000)
  ),
  tar_target(
    small_stat_grids_with_data_res_9,
    stat_grids_with_data[-large_indices_for_res_9],
    iteration = "list"
  ),
  tar_target(
    large_stat_grids_with_data_res_9,
    stat_grids_with_data[large_indices_for_res_9],
    iteration = "list"
  ),
  tar_target(
    small_hex_grids_res_9,
    tail(hex_grids, 376)[-large_indices_for_res_9],
    iteration = "list"
  ),
  tar_target(
    large_hex_grids_res_9,
    tail(hex_grids, 376)[large_indices_for_res_9],
    iteration = "list"
  ),
  tar_target(
    small_pop_units_res_9,
    {
      units <- pop_units
      units <- units[! units$tar_group %in% large_indices_for_res_9, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    large_pop_units_res_9,
    {
      units <- pop_units
      units <- units[units$tar_group %in% large_indices_for_res_9, ]
      units$tar_group <- seq.int(1, nrow(units))
      units <- sf::st_drop_geometry(units)
      units
    },
    iteration = "group"
  ),
  tar_target(
    small_hexagons_with_data_res_9,
    aggregate_data_to_hexagons(
      small_pop_units_res_9,
      small_stat_grids_with_data_res_9,
      small_hex_grids_res_9,
      res = 9,
      manual_parallelization = FALSE
    ),
    pattern = map(
      small_pop_units_res_9,
      small_stat_grids_with_data_res_9,
      small_hex_grids_res_9
    ),
    retrieval = "worker",
    storage = "worker",
    iteration = "list",
    format = "file"
  ),
  tar_target(
    large_hexagons_with_data_res_9,
    aggregate_data_to_hexagons(
      large_pop_units_res_9,
      large_stat_grids_with_data_res_9,
      large_hex_grids_res_9,
      res = 9,
      manual_parallelization = TRUE
    ),
    pattern = map(
      large_pop_units_res_9,
      large_stat_grids_with_data_res_9,
      large_hex_grids_res_9
    ),
    garbage_collection = TRUE,
    iteration = "list",
    format = "file"
  )
)