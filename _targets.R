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

if (!interactive()) future::plan(future.callr::callr, workers = getOption("TARGETS_N_CORES"))

tar_option_set(workspace_on_error = TRUE)

list(
  # basic input
  tar_target(h3_resolutions, 7:9),
  tar_target(
    subset_census_data_path,
    "../../data/acesso_oport/setores_censitarios/dados_censo2010A.csv",
    format = "file"
  ),
  tar_target(
    census_income_data_paths,
    list.files(
      file.path(
        Sys.getenv("IPEA_DATA_PATH"),
        "PUBLICO/CENSO/Setor_Censitario/2010/Originais/"
      ),
      pattern = "Entorno04.+\\.(xls|XLS)$",
      recursive = TRUE,
      full.names = TRUE
    ),
    format = "file"
  ),
  
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
    retrieval = "worker",
    storage = "worker",
    iteration = "list"
  ),
  
  
  tar_target(
    individual_urban_concentrations,
    tar_group(
      dplyr::group_by(
        sf::st_transform(urban_concentrations, 4326),
        code_urban_concentration
      )
    ),
    iteration = "group"
  ),
  tar_target(
    individual_hex_grids,
    create_hex_grids(h3_resolutions, individual_urban_concentrations),
    retrieval = "worker",
    storage = "worker",
    pattern = cross(h3_resolutions, individual_urban_concentrations),
    iteration = "list"
  ),
  tar_target(
    urban_concentration_tracts,
    subset_urban_conc_tracts(census_tracts, urban_concentrations)
  ),
  
  # data processing
  tar_target(subset_census_data, filter_census_data(subset_census_data_path)),
  tar_target(census_income_data, bind_income_data(census_income_data_paths)),
  tar_target(
    processed_census_data,
    process_census_data(subset_census_data, census_income_data)
  ),
  tar_target(
    tracts_with_data,
    merge_census_tracts_data(urban_concentration_tracts, processed_census_data)
  ),
  tar_target(
    individual_tracts_with_data,
    filter_tracts_with_data(tracts_with_data, individual_urban_concentrations),
    retrieval = "worker",
    storage = "worker",
    pattern = map(individual_urban_concentrations),
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
    retrieval = "worker",
    storage = "worker",
    pattern = map(individual_tracts_with_data),
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
    retrieval = "worker",
    storage = "worker",
    pattern = map(small_stat_grids, small_tracts_with_data),
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
        individual_urban_concentrations,
        code_urban_concentration %in% c(3550308, 5300108, 4106902, 3304557)
      )
      indices <- large$tar_group
      indices <- indices[order(indices)]
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
    small_individual_hex_grids_res_7,
    head(individual_hex_grids, 187)[-large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    large_individual_hex_grids_res_7,
    head(individual_hex_grids, 187)[large_indices_for_res_7],
    iteration = "list"
  ),
  tar_target(
    small_individual_urban_concentrations_res_7,
    {
      concs <- individual_urban_concentrations
      concs <- concs[! concs$tar_group %in% large_indices_for_res_7, ]
      concs <- concs[order(concs$tar_group), ]
      concs$tar_group <- seq.int(1, nrow(concs))
      concs <- sf::st_drop_geometry(concs)
      concs
    },
    iteration = "group"
  ),
  tar_target(
    large_individual_urban_concentrations_res_7,
    {
      concs <- individual_urban_concentrations
      concs <- concs[concs$tar_group %in% large_indices_for_res_7, ]
      concs <- concs[order(concs$tar_group), ]
      concs$tar_group <- seq.int(1, nrow(concs))
      concs <- sf::st_drop_geometry(concs)
      concs
    },
    iteration = "group"
  ),
  tar_target(
    small_hexagons_with_data_res_7,
    aggregate_data_to_hexagons(
      small_individual_urban_concentrations_res_7,
      small_stat_grids_with_data_res_7,
      small_individual_hex_grids_res_7,
      res = 7,
      manual_parallelization = FALSE
    ),
    retrieval = "worker",
    storage = "worker",
    pattern = map(
      small_individual_urban_concentrations_res_7,
      small_stat_grids_with_data_res_7,
      small_individual_hex_grids_res_7
    ),
    iteration = "list",
    format = "file"
  ),
  tar_target(
    large_hexagons_with_data_res_7,
    aggregate_data_to_hexagons(
      large_individual_urban_concentrations_res_7,
      large_stat_grids_with_data_res_7,
      large_individual_hex_grids_res_7,
      res = 7,
      manual_parallelization = TRUE
    ),
    garbage_collection = TRUE, 
    pattern = map(
      large_individual_urban_concentrations_res_7,
      large_stat_grids_with_data_res_7,
      large_individual_hex_grids_res_7
    ),
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
    small_individual_hex_grids_res_8,
    individual_hex_grids[188:374][-large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    large_individual_hex_grids_res_8,
    individual_hex_grids[188:374][large_indices_for_res_8],
    iteration = "list"
  ),
  tar_target(
    small_individual_urban_concentrations_res_8,
    {
      concs <- individual_urban_concentrations
      concs <- concs[! concs$tar_group %in% large_indices_for_res_8, ]
      concs <- concs[order(concs$tar_group), ]
      concs$tar_group <- seq.int(1, nrow(concs))
      concs <- sf::st_drop_geometry(concs)
      concs
    },
    iteration = "group"
  ),
  tar_target(
    large_individual_urban_concentrations_res_8,
    {
      concs <- individual_urban_concentrations
      concs <- concs[concs$tar_group %in% large_indices_for_res_8, ]
      concs <- concs[order(concs$tar_group), ]
      concs$tar_group <- seq.int(1, nrow(concs))
      concs <- sf::st_drop_geometry(concs)
      concs
    },
    iteration = "group"
  ),
  tar_target(
    small_hexagons_with_data_res_8,
    aggregate_data_to_hexagons(
      small_individual_urban_concentrations_res_8,
      small_stat_grids_with_data_res_8,
      small_individual_hex_grids_res_8,
      res = 8,
      manual_parallelization = FALSE
    ),
    retrieval = "worker",
    storage = "worker",
    pattern = map(
      small_individual_urban_concentrations_res_8,
      small_stat_grids_with_data_res_8,
      small_individual_hex_grids_res_8
    ),
    iteration = "list",
    format = "file"
  ),
  tar_target(
    large_hexagons_with_data_res_8,
    aggregate_data_to_hexagons(
      large_individual_urban_concentrations_res_8,
      large_stat_grids_with_data_res_8,
      large_individual_hex_grids_res_8,
      res = 8,
      manual_parallelization = TRUE
    ),
    garbage_collection = TRUE, 
    pattern = map(
      large_individual_urban_concentrations_res_8,
      large_stat_grids_with_data_res_8,
      large_individual_hex_grids_res_8
    ),
    iteration = "list",
    format = "file"
  ),
  
  # statistical grid to hexagons res 9
  tar_target(
    hexagons_with_data_res_9,
    aggregate_data_to_hexagons(
      individual_urban_concentrations,
      stat_grids_with_data,
      individual_hex_grids,
      res = 9,
      manual_parallelization = TRUE
    ),
    garbage_collection = TRUE, 
    pattern = map(
      individual_urban_concentrations,
      stat_grids_with_data,
      tail(individual_hex_grids, 187)
    ),
    iteration = "list",
    format = "file"
  )
)