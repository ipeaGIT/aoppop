suppressPackageStartupMessages({
  library(targets)
  library(tarchetypes)
  library(ggplot2)
  library(geoarrow)
  library(parallelly)
  library(duckspatial)
  library(sf)
})

ncores <- floor(.50 * parallelly::freeCores()[1])
options(TARGETS_SHOW_PROGRESS = TRUE, TARGETS_N_CORES = ncores)

control_max_paralelo <- crew::crew_controller_local(
  "max_paralelo",
  workers = getOption("TARGETS_N_CORES")
)

tar_option_set(
  trust_timestamps = TRUE,
  controller = crew::crew_controller_group(
    control_max_paralelo
  ),
  resources = tar_resources(
    crew = tar_resources_crew(controller = "max_paralelo")
  ),
  storage = "worker",
  retrieval = "worker",
  workspace_on_error = TRUE,
  iteration = "list"
)

tar_source()

iter <- data.frame(res = 7:9)

list(
  tar_target(
    name = years,
    command = c(2010, 2022)
  ),

  # spatial manipulation

  tar_target(
    name = census_tracts,
    command = download_census_tracts(years),
    pattern = map(years),
    deployment = "main"
  ),

  tar_target(
    name = census_statistical_grid,
    command = download_statistical_grid(years),
    pattern = map(years),
    deployment = "main"
  ),

  tar_target(
    name = urban_concentrations,
    command = download_urban_concentrations()
  ),

  tar_target(
    name = pop_arrangements,
    command = download_pop_arrangements()
  ),

  tar_target(
    name = pop_units,
    command = merge_pop_units(urban_concentrations, pop_arrangements),
    iteration = "group"
  ),

  tar_target(
    name = pop_units_tracts,
    command = subset_pop_units_tracts(census_tracts, pop_units),
    pattern = map(census_tracts)
  ),
  tar_target(
    name = populated_stat_grids,
    command = prepare_stat_grids(census_statistical_grid, pop_units_tracts),
    pattern = map(census_statistical_grid, pop_units_tracts),
    deployment = "main"
  ),

  # data processing

  tar_target(
    name = census_data,
    command = prepare_census_data(years),
    pattern = map(years)
  ),
  tar_target(
    name = tracts_with_data,
    command = merge_census_tracts_data(pop_units_tracts, census_data),
    pattern = map(pop_units_tracts, census_data)
  ),
  tar_target(
    name = individual_tracts_with_data,
    command = filter_tracts_with_data(years, tracts_with_data, pop_units),
    pattern = cross(map(years, tracts_with_data), pop_units)
  ),

  tar_target(
    name = individual_stat_grids,
    command = filter_individual_stat_grids(
      years,
      populated_stat_grids,
      pop_units,
      individual_tracts_with_data
    ),
    pattern = map(
      cross(map(years, populated_stat_grids), pop_units),
      individual_tracts_with_data
    )
  ),

  # data interpolation

  tar_target(
    name = stat_grids_with_data,
    command = aggregate_data_to_stat_grid(
      years,
      pop_units,
      individual_stat_grids,
      individual_tracts_with_data
    ),
    pattern = map(
      cross(years, pop_units),
      individual_stat_grids,
      individual_tracts_with_data
    )
  ),

  # we cannot create the patterns we want with the pattern argument of
  # tar_target, because we have two "independent" multipliers of pop_units,
  # which are h3_res and years. therefore, we use static branching here to
  # filter only h3 grids of a specific resolution and branch over them to
  # interpolate the data

  tar_map(
    values = iter,
    tar_target(
      name = hex_grids_res,
      command = create_hex_grid(res, pop_units),
      pattern = map(pop_units),
      format = "file"
    ),
    tar_target(
      name = hexs_with_data_res,
      command = aggregate_data_to_hexagons(
        years,
        pop_units,
        stat_grids_with_data,
        hex_grids_res
      ),
      pattern = map(
        cross(years, map(pop_units, hex_grids_res)),
        stat_grids_with_data
      ),
      format = "file"
    )
  )
)
