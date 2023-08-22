options(
  TARGETS_SHOW_PROGRESS = TRUE,
  TARGETS_N_CORES = 30
)

suppressPackageStartupMessages({
  library(targets)
  library(ggplot2)
})

source("R/1_spatial_manipulation.R", encoding = "UTF-8")
source("R/2_data_processing.R", encoding = "UTF-8")
source("R/3_data_interpolation.R", encoding = "UTF-8")

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
    urban_concentration_stat_grid,
    subset_urban_conc_grid(census_statistical_grid, urban_concentrations)
  ),
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
  )
)
