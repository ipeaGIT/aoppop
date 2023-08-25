options(
  TARGETS_SHOW_PROGRESS = TRUE,
  TARGETS_N_CORES = 20
)

suppressPackageStartupMessages({
  library(targets)
  library(ggplot2)
  library(sf)
})

source("R/1_spatial_manipulation.R", encoding = "UTF-8")
source("R/2_data_processing.R", encoding = "UTF-8")
source("R/3_data_interpolation.R", encoding = "UTF-8")
source("R/test.R", encoding = "UTF-8")

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
  tar_target(
    urban_concentration_tracts,
    subset_urban_conc_tracts(census_tracts, urban_concentrations)
  ),
  tar_target(
    urban_conc_hex_grid,
    create_hex_grid(urban_concentrations, h3_resolutions),
    pattern = map(h3_resolutions)
  ),
  
  # data processing
  tar_target(subset_census_data, filter_census_data(subset_census_data_path)),
  tar_target(census_income_data, bind_income_data(census_income_data_paths)),
  tar_target(
    processed_census_data,
    process_census_data(subset_census_data, census_income_data)
  ),
  
  # data interpolation
  tar_target(
    tracts_with_data,
    merge_census_tracts_data(urban_concentration_tracts, processed_census_data)
  ),
  tar_target(
    urban_concentration_stat_grid,
    subset_urban_conc_grid(
      census_statistical_grid,
      urban_concentrations,
      tracts_with_data 
    )
  ),
  
  # experimental
  tar_target(
    individual_urban_concentrations,
    tar_group(dplyr::group_by(sf::st_transform(urban_concentrations, 4326), code_urban_concentration)),
    iteration = "group"
  ),
  tar_target(less_res, 9)
  ,
  tar_target(
    individual_hex_grids,
    {
      urban_conc_cells <- h3jsr::polygon_to_cells(individual_urban_concentrations, less_res)
      urban_conc_grid <- h3jsr::cell_to_polygon(urban_conc_cells, simple = FALSE)
      urban_conc_grid
    },
    retrieval = "worker",
    storage = "worker",
    deployment = "worker",
    memory = "transient",
    garbage_collection = TRUE,
    pattern = cross(less_res, individual_urban_concentrations)
  ),
  tar_target(
    individual_tracts_with_data,
    filter_tracts_with_data(tracts_with_data, individual_urban_concentrations),
    retrieval = "worker",
    storage = "worker",
    deployment = "worker",
    memory = "transient",
    garbage_collection = TRUE,
    pattern = map(individual_urban_concentrations),
    iteration = "list"
  ),
  tar_target(
    statistical_grid_with_pop,
    subset(census_statistical_grid, POP > 0),
    format = "qs"
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
  )
)