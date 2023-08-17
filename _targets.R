options(TARGETS_SHOW_PROGRESS = TRUE)

suppressPackageStartupMessages({
  library(targets)
})

source("R/1_download_census_tracts.R", encoding = "UTF-8")

list(
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
  tar_target(subset_census_data, filter_census_data(subset_census_data_path)),
  tar_target(census_income_data, bind_income_data(census_income_data_paths)),
  tar_target(
    processed_census_data,
    process_census_data(subset_census_data, census_income_data)
  ),
  tar_target(census_tracts, download_census_tracts()),
  tar_target(census_statistical_grid, download_statistical_grid())
)
